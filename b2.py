import requests, hashlib, queue, threading, os, traceback, time
from requests.auth import HTTPBasicAuth


class B2Future:
    def __init__(self, response=None):

        self.lock = threading.Lock()
        self.lock.acquire()

        self.response = response


class B2File:
    def __init__(self, b2_id, name, sha1, upload_timestamp):
        self.b2_id = b2_id
        self.name = name
        self.sha1 = sha1
        self.upload_timestamp = upload_timestamp


class B2Threaded:

    def __init__(self, account_id, application_key, bucket_name, threads, verbose):

        self.verbose = verbose

        self.task_queue = queue.Queue(maxsize=threads)
        self.upload_auth = queue.Queue()
        self.general_auth = queue.Queue()

        self.workers = []
        for i in range(threads):
            w = _B2ThreadedWorker(account_id, application_key, bucket_name, self.task_queue, self.general_auth, self.upload_auth, verbose)
            w.start()
            self.workers.append(w)

        if self.verbose:
            print('B2 threaded running with %d threads' % threads)


    def upload(self, name, source):
        if not isinstance(name, str):
            raise Exception('Upload name must be a string')

        self.task_queue.put(('upload', name, source))


    def delete(self, name, file_id):
        self.task_queue.put(('delete', name, file_id))


    def download(self, name, stream=False):

        future = B2Future()
        self.task_queue.put(('download', name, stream, future))

        return future

    def get_file_data(self, name):

        future = B2Future()
        self.task_queue.put(('get_file_data', name, future))

        return future


    def list_files(self):

        future = B2Future()
        self.task_queue.put(('list', future))

        return future


    def shutdown(self):

        for w in self.workers:
            self.task_queue.put('shutdown')

        if self.verbose:
            print('B2 threaded worker threads shutting down...')

        for w in self.workers:
            w.join()

        if self.verbose:
            print('B2 threaded worker threads shutdown complete.')


    def wait_on_all_complete(self):
        self.task_queue.join()


class _B2ThreadedWorker(threading.Thread):

    def __init__(self, account_id, application_key, bucket_name, task_queue, general_auth, upload_auth, verbose):
        threading.Thread.__init__(self)

        self.verbose = verbose

        self.task_queue = task_queue
        self.general_auth = general_auth
        self.upload_auth = upload_auth

        self.account_id = account_id
        self.application_key = application_key
        self.bucket_name = bucket_name
        self.bucket_id = None

        self.requests_session = requests.Session()


    def run(self):
        try:
            while True:
                task = self.task_queue.get()

                if task[0] == 'upload':
                    self._upload(task)

                if task[0] == 'delete':
                    self._delete_file(task)

                if task[0] == 'download':
                    self._download(task)

                if task[0] == 'get_file_data':
                    self._get_file_data(task)

                if task[0] == 'list':
                    self._list(task)

                if task == 'shutdown':
                    self.task_queue.task_done()
                    break

                self.task_queue.task_done()

        except Exception as e:
            print("Unhandled exception on B2 thread, hard failing.")
            traceback.print_exc()
            os._exit(1)


    def _delete_file(self, task):
        name = task[1]
        file_id = task[2]

        if self.verbose:
            print('Deleting ' + str(name))

        url = '/b2api/v1/b2_delete_file_version'
        r = self._request('POST', url=url, json={'fileName':name, 'fileId':file_id}, need_gen_auth=True)

        if self.verbose and r and r.status_code == 400 and 'code' in r.json() and r.json()['code'] == 'file_not_present':
            print('File not found, continuing as if deleted: ', name)

        if self.verbose:
            print('Deletion complete: ', name)


    def _upload(self, task):

        name = task[1]
        source = task[2]

        data_hash = self._sha1(source)

        headers = {
            'X-Bz-File-Name': name,
            'Content-Type': 'application/octet-stream',
            'X-Bz-Content-Sha1': data_hash
        }

        self._request('POST', headers=headers, data=source, is_upload=True)


    def _download(self, task):
        name = task[1]
        stream = task[2]
        future = task[3]

        url = '/file/%s/%s' % (self.bucket_name, name)
        r = self._request('GET', url, is_download=True, stream=stream)

        if r.status_code == requests.codes.not_found:
            resp = None
        elif stream:
            resp = r.raw
        else:
            resp = r.content
            r.close()

        future.response = resp
        future.lock.release()


    def _get_file_data(self, task):
        name = task[1]
        future = task[2]

        url = '/file/%s/%s' % (self.bucket_name, name)
        r = self._request('HEAD', url, is_download=True)

        if r.status_code == requests.codes.not_found:
            b2_file = None
        else:
            b2_file = B2File(r.headers['x-bz-file-id'], name, r.headers['x-bz-content-sha1'], r.headers['X-Bz-Upload-Timestamp'])
            r.close()

        future.response = b2_file
        future.lock.release()


    def _list(self, task):
        future = task[1]

        if self.verbose:
            print('Getting bucket contents list')

        files = []
        req_data = {'bucketId': self._get_bucket_id(), 'maxFileCount': 1000}
        while True:

            url = '/b2api/v1/b2_list_file_versions'
            r = self._request('POST', url, json=req_data, need_gen_auth=True)

            resp_data = r.json()

            for file_data in resp_data['files']:
                files.append(B2File(file_data['fileId'], file_data['fileName'], file_data['contentSha1'], file_data['uploadTimestamp']))

            if resp_data['nextFileId']:
                req_data['startFileId'] = resp_data['nextFileId']
                req_data['startFileName'] = resp_data['nextFileName']

                if self.verbose:
                    print('Got', len(files), 'objects so far, continuing')
            else:
                future.response = files
                future.lock.release()
                if self.verbose:
                    print('Got file list, total',len(files))
                break



    def _get_bucket_id(self):

        if self.bucket_id:
            return self.bucket_id

        url = '/b2api/v1/b2_list_buckets'
        r = self._request('GET', url, params={'accountId': self.account_id}, need_gen_auth=True)

        bucket_id = [b['bucketId'] for b in r.json()['buckets'] if b['bucketName'] == self.bucket_name][0]
        if not bucket_id:
            print('Bucket name not found. Hard failing.')
            os._exit(1)

        self.bucket_id = bucket_id
        return bucket_id


    def _get_upload_auth(self):

        try:
            return self.upload_auth.get_nowait()
        except queue.Empty:

            url = '/b2api/v1/b2_get_upload_url'
            r = self._request('POST', url, json={'bucketId': self._get_bucket_id()}, need_gen_auth=True)

            if self.verbose:
                print('B2 got new auth for upload')

            return r.json()


    def _get_general_auth(self):

        try:
            return self.general_auth.get_nowait()
        except queue.Empty:

            url = 'https://api.backblazeb2.com/b2api/v1/b2_authorize_account'
            r = self._request('GET', url=url, auth=HTTPBasicAuth(self.account_id, self.application_key))

            if self.verbose:
                print('B2 got new auth for general operations')

            return r.json()


    def _request(self, method, url=None, auth=None, headers=None, json=None, params=None, data=None, need_gen_auth=False, is_upload=False, is_download=False, stream=False):

        if is_upload and need_gen_auth:
            raise RuntimeError('BUG: Cannot need both gen and upload auth at the same time.')

        if not headers:
            headers = {}

        failures=0
        while True:

            url_prefix = ''
            gen_auth = None
            if need_gen_auth or is_download:
                gen_auth = self._get_general_auth()
                headers['Authorization'] = gen_auth['authorizationToken']
                if is_download:
                    url_prefix = gen_auth['downloadUrl']
                else:
                    url_prefix = gen_auth['apiUrl']

            upload_auth = None
            if is_upload:
                upload_auth = self._get_upload_auth()
                headers['Authorization'] = upload_auth['authorizationToken']
                url = upload_auth['uploadUrl']

            if not url.startswith('http'):
                url = url_prefix + url

            r = None
            try:
                r = self.requests_session.request(method, url, auth=auth, headers=headers, params=params, json=json, data=data, stream=stream)
            except:
                if self.verbose:
                    print("Requests error")
                    traceback.print_exc()

            if r is None or (r.status_code != requests.codes.ok
                         and r.status_code != requests.codes.bad_request #400 is unrecoverable, so return it anyway for other code to handle (or not)
                         and r.status_code != requests.codes.not_found #404 also unrecoverable
                         and r.status_code != requests.codes.forbidden): #handled below

                failures += 1
                self._handle_retry_backoff(r, failures)

                if hasattr(data, 'seek'): #otherwise will upload nothing on retry
                    data.seek(0)

            elif r.status_code == requests.codes.forbidden:
                print('B2 account cap limit reached or account in bad standing. Please review your B2 account! Backup has failed.')
                os._exit(1)

            else:
                #reuse auth only on success
                if gen_auth:
                    self.general_auth.put(gen_auth)
                if upload_auth:
                    self.upload_auth.put(upload_auth)

                return r


    def _handle_retry_backoff(self, r, failures):
        if failures > 100:
            print('Too many network failures, hard failing!')
            os._exit(1)

        if r and 'Retry-After' in r.headers:
            backoff = int(r.headers['Retry-After'])
        else:
            backoff = 2 ** failures - 1

        if self.verbose:
            if r:
                print('HTTP bad status code %d\n%s' % (r.status_code, r.text))
            print('Waiting %s seconds for retry' % backoff)

        time.sleep(backoff)

        if self.verbose:
            print('Retrying...')


    def _sha1(self, source):
        sha1 = hashlib.sha1()

        if isinstance(source, bytes):
            sha1.update(source)
        else:
            if source.tell() != 0:
                raise RuntimeError('BUG: upload source file not at 0 position. Failing.')

            while True:
                buf = source.read(8 * 1024)
                if len(buf) == 0:
                    break
                sha1.update(buf)

            source.seek(0)

        return sha1.hexdigest()



