from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from datetime import datetime as dt

CLIENT_SECRETS_FILE = './credentials.json'
SCOPES = ['https://www.googleapis.com/auth/drive']
API_SERVICE_NAME = 'drive'
API_VERSION = 'v3'


def main():
    service = authenticate()

    files = generate_files(service)

    # Find duplicates if same filename and parent
    print(f'Generating list of duplicate files... 0 duplicates in {len(files)} files', end='\r')
    filenames = [file['name'] for file in files]
    parents = {}
    for file in files:
        # Print problematic file
        try:
            parents.setdefault(file['name'], []).append(file['parents'])
        except KeyError:
            print(file)
    duplicates = {}
    for file in files:
        if filenames.count(file['name']) > 1 and parents[file['name']].count(file['parents']) > 1:
            duplicates.setdefault(file['name'], []).append(file)

            print(
                f'Generating list of duplicate files... {len(duplicates.values())} '
                f'duplicates in {len(files)} files', end='\r')

    print('\n')

    if duplicates:
        # Print duplicates
        for filename, file_list in duplicates.items():
            print(filename)
            for file in file_list:
                print(_format(file, name=False))

        print('')

        deletions = queue_for_deletion(duplicates)

        # Assert user confirmation
        assertion = input('\nProceed? Y/n\n')
        print('')
        if assertion.lower() == 'y':
            for file in deletions:
                print(_format(file))
                service.files().delete(fileId=file['id']).execute()
            # requests = [service.files().delete(file['id']) for file in deletions]
            # batch_requests = _batch(service, requests)

            print('\nFinished')
        else:
            print('Cancelled')
    else:
        print('No duplicates found!')


def _format(file, name=True):
    if name:
        return f'{file["name"]} | {file["id"]} | modified {file["modifiedTime"]}'
    else:
        return f'    {file["id"]} modified {file["modifiedTime"]}'


# def _batch(service, requests):
#     def callback(rid, resp, exc):
#         if exc:
#             print(exc)
#
#     batch = service.new_batch_http_request(callback=callback)
#     for r, i in zip(requests, range(100)):
#         batch.add(r)
#
#     return batch


def authenticate():
    print('Initializing OAuth flow...\n')

    flow = InstalledAppFlow.from_client_secrets_file(
        CLIENT_SECRETS_FILE,
        scopes=SCOPES)
    credentials = flow.run_local_server(
        host='localhost',
        port=8080,
        authorization_prompt_message='Please visit this URL:\n{url}\n',
        success_message='The auth flow is complete; you may close this tab.',
        open_browser=True)

    print('AUTHENTICATED\n')

    return build(API_SERVICE_NAME, API_VERSION, credentials=credentials)


def generate_files(service):
    files = []
    page_token = None
    c = 1

    print('Generating list of all files...', end='\r')

    while(True):
        response = service.files().list(
            pageSize=1000,
            pageToken=page_token,
            fields='nextPageToken, files(id, name, parents, shared, modifiedTime)').execute()
        files.extend(response['files'])

        page_token = response.get('nextPageToken')
        if not page_token:
            break

        print(f'Requesting list of all files... {c * 460}', end='\r')

        c += 1
    print('')

    for file in files:
        if file['shared'] is True:
            files.remove(file)

    return files


def queue_for_deletion(duplicates):
    deletions = []
    for filename, file_list in duplicates.items():
        for file in file_list:
            time = dt.strptime(file['modifiedTime'], '%Y-%m-%dT%H:%M:%S.%fZ')

            for ffile in file_list:
                ttime = dt.strptime(ffile['modifiedTime'], '%Y-%m-%dT%H:%M:%S.%fZ')
                if time < ttime:
                    if file not in deletions:
                        print(_format(file))
                        deletions.append(file)

    return deletions


if __name__ == '__main__':
    main()
