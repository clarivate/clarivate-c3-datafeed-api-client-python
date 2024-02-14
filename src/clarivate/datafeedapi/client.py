#!/usr/bin/env python

import time
from datetime import datetime
import os  
import sys
from pathlib import Path
import argparse
import asyncio
import json
import shutil
import httpx
import glob
import traceback
from urllib.parse import urlparse
from multiprocessing import Pool
from platformdirs import user_config_dir
from typing import Tuple

# Autovivify our application's private directory
MODULE_DIR = Path(__file__).parent
PRIVATE_DIR = Path(user_config_dir("datafeedapi", "clarivate"))
if not PRIVATE_DIR.exists():
    os.makedirs(PRIVATE_DIR)
else:
    if not PRIVATE_DIR.is_dir():
        sys.stderr.write(f"Error: {PRIVATE_DIR} is a file, but should be a directory!\n")
        sys.exit(1)
if not (PRIVATE_DIR / "config.py").exists():
    shutil.copy(MODULE_DIR / "config.py", PRIVATE_DIR)

# Import our config
sys.path.insert(0, str(PRIVATE_DIR.absolute()))
import config
sys.path=sys.path[1:]

httpx_async = httpx.AsyncClient()


# Helper classes & helper functions for performing HTTP calls

class HTTPResponseNotOK(Exception):
    code = 500
    message = ""
    def __init__(self, code: int, message: str):
        self.code=code
        self.message=message
    def __str__(self):
        return self.message

def _retry_exception_logic(e: Exception, ttl: int):
    retry_wait = config.RETRY_WAIT
    if isinstance(e, HTTPResponseNotOK):
        if e.code in range(400, 500): # most 4xx messages are genuinely client problems and ought to be fatal
            raise e
        elif e.code in range(500, 600): # 5xx errors are mostly transient, but increase delay to avoid overloading remote server
            retry_wait *= 2
    elif isinstance(e, httpx.TransportError):
        retry_wait = int(retry_wait*1.5) # transport errors are generally transient, so retry
    else:
        raise e
    if ttl >= config.RETRIES:
        sys.stderr.write("Out of retries; giving up.\n")
        raise e
    return retry_wait

def _retry(func):
    def wrapper_retry(*args, **kwargs):
        for ttl in range(0, config.RETRIES):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                retry_wait = _retry_exception_logic(e, ttl)
                sys.stderr.write(f"Retrying (attempt {ttl+1} of {config.RETRIES}) after {retry_wait}s...\n")
                time.sleep(retry_wait)
                sys.stderr.write("Retrying\n")
    return wrapper_retry

def _retry_async(func):
    async def wrapper_retry(*args, **kwargs):
        for ttl in range(0, config.RETRIES):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                retry_wait = _retry_exception_logic(e, ttl)
                sys.stderr.write(f"Retrying (attempt {ttl+1} of {config.RETRIES}) after {retry_wait}s...\n")
                await asyncio.sleep(retry_wait)
                sys.stderr.write("Retrying\n")
    return wrapper_retry

class MissingAuth(Exception):
    def __str__(self):
        return "No API key found! Please specify as the environment variable DATAFEED_API_KEY or pass to Client initializer."

class Client():
    """ A client for the Clarivate DataFeed API """
    def __init__(self, api_key: str = None, server_url: str = None):
        self.api_key = api_key or config.API_KEY
        self.server_url = server_url or config.SERVER_URL
        if not self.api_key:
            raise MissingAuth 

    def _setup_headers(self, content_type: str):
        headers = {'X-ApiKey': self.api_key}
        if content_type == 'text':
            headers['Content-Type']='application/json'
        return headers

    def _process_response(self, r: httpx.Response, content_type: str):
        response = None
        if r.status_code == 200:
            if content_type == 'binary':
                response = r.content
            else:
                response = r.text
        else:
            raise HTTPResponseNotOK(r.status_code, r.text)
        return response

    @_retry
    def _run_call_sync(
        self,
        pg: str,
        url: str,
        payload: str,
        content_type: str = "text"
        ):
        return self._process_response(httpx.request(pg, url, headers=self._setup_headers(content_type), data=payload), content_type)

    @_retry_async
    async def _run_call_async(
        self,
        pg: str,
        url: str,
        payload: str,
        content_type: str = "text"
        ):
        return self._process_response(await httpx_async.request(pg, url, headers=self._setup_headers(content_type), data=payload), content_type)

    # requestPackage

    def _build_request_package_payload(
            self,
            content_set_name: str,
            preset_content_set_name: str,
            internal_change_number: int = 0,
            output_format: str = 'json',
            split_files: bool = False
            ):
        payload_request = {
                "contentSet": content_set_name,
                "format": output_format,
                "preset": preset_content_set_name,
                "splitFiles": split_files
            }
        if internal_change_number > 0:
            payload_request["filters"]=[
                        {
                            "field": "internalChangeNumber",
                            "op": "gt",
                            "value": internal_change_number
                        }
                    ]
        return json.dumps(payload_request)

    def request_package_sync(
            self,
            content_set_name: str,
            preset_content_set_name: str,
            internal_change_number: int = 0,
            output_format: str = 'json',
            split_files: bool = False
            ):
        """ Request that datafeed begin packaging a contentset for download (sync version) """
        payload = self._build_request_package_payload(content_set_name, preset_content_set_name, internal_change_number, output_format, split_files)
        response = self._run_call_sync("POST", f"{self.server_url}requestPackage", payload, 'text')
        data_token = json.loads(response)
        return data_token['token']

    async def request_package_async(
            self,
            content_set_name: str,
            preset_content_set_name: str,
            internal_change_number: int = 0,
            output_format: str = 'json',
            split_files: bool = False
            ):
        """ Request that datafeed begin packaging a contentset for download (async version) """
        payload = self._build_request_package_payload(content_set_name, preset_content_set_name, internal_change_number, output_format, split_files)
        response = await self._run_call_async("POST", f"{self.server_url}requestPackage", payload, 'text')
        data_token = json.loads(response)
        return data_token['token']


    # checkPackageStatus

    def check_package_status_sync(self, token: str):
        """ Poll for package availability, exactly once (sync version) """
        files = None
        status = None
        response = self._run_call_sync("GET", f"{self.server_url}checkPackageStatus?token={token}", {}, 'text')
        print(f"type(response)={type(response)} response={response}", flush=True)
        data = json.loads(response)
        if 'status' in data:
            status = data['status']
            if status == 'done':
                files = data['files']
        return status, files

    async def check_package_status_async(self, token: str):
        """ Poll for package availability, exactly once (async version) """
        files = None
        status = None
        response = await self._run_call_async("GET", f"{self.server_url}checkPackageStatus?token={token}", {}, 'text')
        data = json.loads(response)
        if 'status' in data:
            status = data['status']
            if status == 'done':
                files = data['files']
        return status, files


    # downloadPackage

    def get_files_sync(self, token: str):
        """ Poll for package file availability and return a status and file list when perparation is finished (sync version) """
        status, files = self.check_package_status_sync(token)
        while status not in ['done', 'done-no-match', 'error']:
            time.sleep(config.POLL_WAIT)
            status, files = self.check_package_status_sync(token)
        return status, files

    async def get_files_async(self, token: str):
        """ Poll for package file availability and return a status and file list when perparation is finished (async version) """
        status, files = await self.check_package_status_async(token)
        while status not in ['done', 'done-no-match', 'error']:
            await asyncio.sleep(config.POLL_WAIT)
            status, files = await self.check_package_status_async(token)
        return status, files

    def download_package_file_sync(
        self,
        file_name: str,
        token: str
        ):
        """ Download a content set package file (sync version) """
        response = self._run_call_sync("GET", f"{self.server_url}downloadPackage?token={token}", {}, 'binary')
        open(file_name, 'wb').write(response)

    async def download_package_file_async(
        self,
        file_name: str,
        token: str
        ):
        """ Download a content set package file (async version) """
        response = await self._run_call_async("GET", f"{self.server_url}downloadPackage?token={token}", {}, 'binary')
        open(file_name, 'wb').write(response)

    def download_package_files_sync(self, destination_directory: str, files: list):
        """ Download all content set package files (sync version) """
        for f in files:
            self.download_package_file_sync(Path(destination_directory)/f['fileName'], f['token'])

    async def download_package_files_async(self, destination_directory: str, files: list):
        """ Download all content set package files (async version) """
        tasks=[]
        i=0
        for f in files:
            tasks.append(asyncio.create_task(self.download_package_file_async(Path(destination_directory)/f['fileName'], f['token'])))
            i+=1
            if i>config.MAX_CONNECT:
                for task in tasks:
                    await task
                tasks=[]
                i=0
        for task in tasks:
            await task


    # Downloading resource files

    @_retry
    def download_file_sync(self, url_structure: Tuple[str, str]):
        """ Download resource file (sync version) 

        The url_structure parameter is a tuple (resource_directory_path, url)
        """
        (path, url) = url_structure
        response = httpx.get(url)
        open(f'{path}/{os.path.basename(urlparse(url).path)}', 'wb').write(response.content)

    @_retry_async
    async def download_file_async(self, url_structure: Tuple[str, str]):
        (path, url) = url_structure
        response = await httpx_async.get(url)
        open(f'{path}/{os.path.basename(urlparse(url).path)}', 'wb').write(response.content)

    def get_resource_list(self, dest_folder: str):
        """ Determine the URLs of downloadable resource files, based on already-downloaded package files """
        arr_urls = []
        #Find *_resources_*.json file
        nameList = glob.glob(f'{dest_folder}/*_resources_*.json')
        if len(nameList) > 0:
            #Create parent resources folder
            os.makedirs(f'{dest_folder}/resources/', exist_ok=True)

            #Load the resource file - we may have several resources files, one per entity/sub-entity
            for res_file in nameList:
                entity_name = res_file.split('_')[0]
                resource_folder = f'{dest_folder}/resources/{entity_name}'
                os.makedirs(resource_folder, exist_ok=True)
                with open(res_file) as f:
                    data = json.load(f)
                    for u in data:
                        arr_urls.append((resource_folder,u['url']))
        return arr_urls

    def extract_resources_sync(self, dest_folder: str):
        """ Download all resource files (sync version) """
        #download all binaries before expiration
        with Pool(processes=config.MAX_CONNECT) as pool:
            pool.map(self.download_file_sync, self.get_resource_list(dest_folder))

    async def _pool_tasks(self, func, param_list: list):
        tasks=[]
        i=0
        for item in param_list:
            tasks.append(asyncio.create_task(func(item)))
            i+=1
            if i>config.MAX_CONNECT:
                for task in tasks:
                    await task
                tasks=[]
                i=0
        for task in tasks:
            await task

    async def extract_resources_async(self, dest_folder: str):
        await self._pool_tasks(self.download_file_async, self.get_resource_list(dest_folder))


    # fetch() and fetch_sync() perform the entire process of fetching a content set to disk

    def fetch_sync(
                self,
                content_set_name: str,
                preset_content_set_name: str,
                internal_change_number: int = 0,
                output_format: str = 'json',
                split_files: bool = False,
                extract_resources: bool = False
            ):
        """ 
        Fetch a content set by name, producing files on disc.

        This is our 'do-everything' function; most users of the
        library will want to use only this function or its async
        equivalent.
        """
        files = None
        token = self.request_package_sync(content_set_name, preset_content_set_name, internal_change_number, output_format, split_files)
        if token is not None:
            #create the destination folder
            dest_folder = f"{config.OUT_DIR}/{content_set_name}_{token}"
            os.makedirs(dest_folder, exist_ok=True)

            #get the list of files to be downloaded
            status, files = self.get_files_sync(token)

        else:
            sys.stderr.write('FAILURE: Issue with token\n')

        if files is not None:
            #Step 3.1 - download all the generated files
            self.download_package_files_sync(dest_folder, files)

            #Step 3.2 - download resources if requested
            if extract_resources:
                self.extract_resources_sync(dest_folder)
        else:
            sys.stderr.write('FAILURE: Download packaged files: no file for download found!\n')
        return dest_folder

    def fetch(
                self,
                content_set_name: str,
                preset_content_set_name: str,
                internal_change_number: int = 0,
                output_format: str = 'json',
                split_files: bool = False,
                extract_resources: bool = False
            ):
        """ 
        Fetch a content set by name, producing files on disc.

        This is our 'do-everything' function; most users of the
        library will want to use only this function or its sync
        equivalent.
        """
        async def fetch_async():
            files = None
            token = await self.request_package_async(content_set_name, preset_content_set_name, internal_change_number, output_format, split_files)
            if token is not None:
                #create the destination folder
                dest_folder = f"{config.OUT_DIR}/{content_set_name}_{token}"
                os.makedirs(dest_folder, exist_ok=True)

                #get the list of files to be downloaded
                status, files = await self.get_files_async(token)

            else:
                sys.stderr.write('FAILURE: Issue with token\n')

            if files is not None:
                #Step 3.1 - download all the generated files
                await self.download_package_files_async(dest_folder, files)

                #Step 3.2 - download resources if requested
                if extract_resources:
                    await self.extract_resources_async(dest_folder)
            else:
                sys.stderr.write('FAILURE: Download packaged files: no file for download found!\n')
            return dest_folder
        return asyncio.run(fetch_async())


def main():
    available_content_sets = [
            {"contentSet": "competitivePatents", "preset": "full", "name": "Competitive Patent Families - Full", "note":"Very big contentSet (>12GB) ;No resource/binary"},
            {"contentSet": "genericsManufacturing", "preset": "standard", "name":"Generics Manufacturing - Standard ", "note":"No resource/binary"},
            {"contentSet": "preclinicalSafetyAlerts", "preset": "full", "name":"Preclinical Safety Alerts - Full", "note":"No resource/binary"},
            {"contentSet": "regulatoryProductsApprovals", "preset": "standard", "name":"Regulatory Products Approvals - Standard ", "note":"A lot of resources/Binaries"},
            {"contentSet": "regulatoryIntelligenceData", "preset": "full", "name":"Regulatory Intelligence Data - Full", "note":"A lot of resources/Binaries"},
            {"contentSet": "competitiveBioworldFinancings", "preset": "standard", "name":"BioWorld Financings - Standard", "note":"No resource/binary"},
            {"contentSet": "understandDLFReports", "preset": "standard", "name":"Understand Disease Landscape & Forecast - Standard", "note":"contains resources/binaries"},
            {"contentSet": "regulatoryCMC", "preset": "standard", "name":"Regulatory CMC - Standard", "note":"contains resources/Binaries"},
            {"contentSet": "competitiveDTSR", "preset": "standard", "name":"Competitive DTSR - Standard"},
            {"contentSet": "understandEPIReports", "preset": "standard", "name":"Understand Epidemiology - Standard", "note":"only EPI data Slicer"},
    ]
    contentset_desc = ["AVAILABLE CONTENTSETS\n\tCONTENTSET\t\tPRESET\tNAME\n"]
    for i in available_content_sets:
        contentset_desc.append(f"\t{i['contentSet']}\t{i['preset']}\t{i['name']}\n")
        if 'note' in i:
            contentset_desc.append(f"\t\t\t\tNote: {i['note']}\n\n")

    parser = argparse.ArgumentParser(
            description="Bulk download IP datasets",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="".join(contentset_desc))
    parser.add_argument("--internal-change-number", dest="changenumber", default=0, type=int, help="Select only data newer than this internal change number (default: 0)")
    parser.add_argument("--format", dest="fmt", default="json", choices=["json", "parquet"], help="Format for the downloaded files, either json or parquet (default: json)")
    parser.add_argument("--split", dest="split", action="store_true", help="Split files")
    parser.add_argument("--extract", dest="extract", action="store_true", help="Download and extract resources (may be slow)")
    parser.add_argument("contentset", metavar="CONTENTSET:PRESET", nargs="+", help="The name of the contentset and its preset")
    args=parser.parse_args()

    cspairs=[x.split(":") for x in args.contentset]
    for cs in cspairs:
        if len(cs) != 2:
            parser.print_help()
            sys.exit(1)

    for cs in cspairs:
        start = time.time()
        now = datetime.now()
        print('Process started at %s....' %(now))

        try:
            Client().fetch(
                cs[0],
                cs[1],
                args.changenumber,
                args.fmt,
                args.split,
                args.extract
            )
        except Exception as e:
            sys.stderr.write("ERROR\n")
            if isinstance(e, MissingAuth):
                sys.stderr.write(f"{e}\n")
            elif isinstance(e, HTTPResponseNotOK):
                sys.stderr.write(f"{e.message}\n")
            else:
                sys.stderr.write("An unexpected error occurred. Please report the details below to your Clarivate contact.\n\n")
                sys.stderr.write(f"parameters={sys.argv}")
                sys.stderr.write(traceback.format_exc())
            sys.exit(1)

        elapsed = (time.time() - start)/60
        print('Process completed! time taken: %s minutes' %(str(elapsed)))

if __name__ == '__main__':
    main()
