#!/usr/bin/env python
import logging
import argparse
import asyncio
import glob
import json
import os
import shutil
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Literal, Coroutine
from urllib.parse import urlparse

from httpx import HTTPStatusError, AsyncClient, TransportError, Response
from platformdirs import user_config_dir


LOGGER = logging.getLogger("clarivate.datafeedapi")

# When a new variable is added or existing changed to the config file, It should be available to client.py
# Start
from clarivate.datafeedapi import config as app_config

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

# Import user config
sys.path.insert(0, str(PRIVATE_DIR.absolute()))
import config

app_config_vars = {key: value for key, value in app_config.__dict__.items() if key.isupper()}
user_config_vars = {key: value for key, value in config.__dict__.items() if key.isupper()}

for key, value in app_config_vars.items():
    if key not in user_config_vars:
        config.__dict__[key] = value

sys.path = sys.path[1:]
# End


# Helper classes & helper functions for performing HTTP calls
class HTTPResponseNotOK(Exception):
    code = 500
    message = ""

    def __init__(self, code: int, message: str):
        self.code = code
        self.message = message

    def __str__(self):
        return self.message


def _retry_exception_logic(e: Exception, ttl: int) -> float:
    if ttl >= config.RETRIES:
        LOGGER.error("Out of retries; giving up")
        raise HTTPResponseNotOK(e.response.status_code, e.response.text) if isinstance(e, HTTPStatusError) else e

    if isinstance(e, TransportError):
        return config.RETRY_WAIT * 1.5

    if isinstance(e, HTTPStatusError):
        if 400 <= e.response.status_code < 500:
            # most 4xx messages are genuinely client problems and ought to be fatal
            raise HTTPResponseNotOK(e.response.status_code, e.response.text)
        elif 500 <= e.response.status_code < 600:
            # 5xx errors are mostly transient, but increase delay to avoid overloading remote server
            return config.RETRY_WAIT * 2
    raise e


def _retry_async(func):
    async def wrapper_retry(*args, **kwargs):
        for ttl in range(0, config.RETRIES):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                LOGGER.error("Request failed with error",  e)

                retry_wait = _retry_exception_logic(e, ttl)
                LOGGER.error(
                    "Retrying (attempt %s of %s) after %s s...",
                    ttl+1, config.RETRIES, retry_wait
                )
                await asyncio.sleep(retry_wait)
                LOGGER.error("Retrying")

    return wrapper_retry


def _async_to_sync(awaitable: Coroutine) -> Any:
    return asyncio.run(awaitable)


async def _pool_tasks(func: Any, param_list: list[tuple[Any, ...]]) -> None:
    tasks = []

    for item in param_list:
        tasks.append(asyncio.create_task(func(*item)))

        if len(tasks) >= config.MAX_CONNECT:
            await asyncio.gather(*tasks)
            tasks = []

    if tasks:
        await asyncio.gather(*tasks)


def get_file_length(file_path: str) -> int:
    """Get the total bytes of the file."""
    if not os.path.exists(file_path):
        return 0
    return os.path.getsize(file_path)


class MissingAuth(Exception):
    def __str__(self):
        return "No API key found! Please specify as the environment variable DATAFEED_API_KEY or pass to Client initializer."


class Client:
    """A client for the Clarivate DataFeed API"""

    def __init__(self, api_key: Optional[str] = None, server_url: Optional[str] = None) -> None:
        self.api_key = api_key or config.API_KEY
        self.server_url = (server_url or config.SERVER_URL).rstrip("/") + "/"
        if not self.api_key:
            raise MissingAuth

    @property
    def _async_client(self) -> AsyncClient:
        # create new client because it is coupled with async event loop
        return AsyncClient(timeout=120)

    def _setup_headers(
        self,
        content_type: Literal["binary", "text"],
        current_chunk_length: int = 0
    ) -> dict[str, str]:
        headers = {"X-ApiKey": self.api_key}
        if content_type == "text":
            headers["Content-Type"] = "application/json"
        if current_chunk_length > 0:
            headers.update({"Range": f"bytes={current_chunk_length}-"})
        return headers

    def _process_json_response(self, r: Response) -> dict:
        if r.is_success:
            return r.json()

        r.raise_for_status()
        return {}

    @_retry_async
    async def _run_call_async(
        self,
        pg: str,
        url: str,
        payload: Optional[dict[str, Any]] = None
    ) -> dict[str, Any]:
        return self._process_json_response(
            await self._async_client.request(
                pg,
                url,
                headers=self._setup_headers("text"),
                json=payload,
                follow_redirects=True
            )
        )

    # contentSets info
    def get_content_sets_info_sync(self) -> list[dict[str, str]]:
        return _async_to_sync(self.get_content_sets_info_async())

    async def get_content_sets_info_async(self) -> list[dict[str, str]]:
        response = await self._run_call_async(
            "GET", f"{self.server_url}info"
        )
        return response["subscriptions"]

    # requestPackage
    def _build_request_package_payload(
        self,
        content_set_name: str,
        preset_content_set_name: str,
        internal_change_number: int = 0,
        output_format: Literal["json", "parquet"] = "json",
        split_files: bool = False,
    ) -> dict[str, Any]:
        payload_request = {
            "contentSet": content_set_name,
            "format": output_format,
            "preset": preset_content_set_name,
            "splitFiles": split_files,
        }
        if internal_change_number > 0:
            payload_request["filters"] = [
                {"field": "internalChangeNumber", "op": "gt", "value": internal_change_number}
            ]
        return payload_request

    def request_package_sync(
        self,
        content_set_name: str,
        preset_content_set_name: str,
        internal_change_number: int = 0,
        output_format: Literal["json", "parquet"] = "json",
        split_files: bool = False,
    ) -> str:
        """Request that datafeed begin packaging a contentset for download (sync version)"""
        return _async_to_sync(self.request_package_async(
            content_set_name=content_set_name,
            preset_content_set_name=preset_content_set_name,
            internal_change_number=internal_change_number,
            output_format=output_format,
            split_files=split_files
        ))

    async def request_package_async(
        self,
        content_set_name: str,
        preset_content_set_name: str,
        internal_change_number: int = 0,
        output_format: Literal["json", "parquet"] = "json",
        split_files: bool = False,
    ) -> str:
        """Request that datafeed begin packaging a contentset for download (async version)"""
        LOGGER.info("Requesting content set '%s' preset '%s' ...", content_set_name, preset_content_set_name)
        payload = self._build_request_package_payload(
            content_set_name,
            preset_content_set_name,
            internal_change_number,
            output_format,
            split_files,
        )
        response = await self._run_call_async(
            "POST", f"{self.server_url}requestPackage", payload
        )

        if "token" not in response or not response["token"]:
            raise ValueError(f"Unexpected response {json.dumps(response)} for request {json.dumps(payload)}")

        token = response["token"]
        LOGGER.info("Request is successful, token is %s", token)
        return token

    # checkPackageStatus

    def check_package_status_sync(self, token: str) -> tuple[str, list[dict]]:
        """Poll for package availability, exactly once (sync version)"""
        return _async_to_sync(self.check_package_status_async(token=token))

    async def check_package_status_async(self, token: str) -> tuple[Optional[str], Optional[list[dict]]]:
        """Poll for package availability, exactly once (async version)"""
        LOGGER.info("Checking status of token %s ...", token)
        files = None
        status = None
        response = await self._run_call_async(
            "GET", f"{self.server_url}checkPackageStatus?token={token}"
        )
        if "status" in response:
            status = response["status"]
            if status == "done":
                files = response["files"]
        LOGGER.info("Status is '%s'", status)
        return status, files

    # downloadPackage

    def get_files_sync(self, token: str) -> tuple[str, list[dict]]:
        """Poll for package file availability and return a status and file list when perparation is finished (sync version)"""
        return _async_to_sync(self.get_files_async(token=token))

    async def get_files_async(self, token: str) -> tuple[Optional[str], Optional[list[dict]]]:
        """Poll for package file availability and return a status and file list when perparation is finished (async version)"""
        status, files = await self.check_package_status_async(token)
        while status not in ["done", "done-no-match", "error"]:
            LOGGER.info("Waiting %s s ...", config.POLL_WAIT)
            await asyncio.sleep(config.POLL_WAIT)
            status, files = await self.check_package_status_async(token)
        return status, files

    def download_package_file_sync(self, file_name: str, token: str) -> None:
        return _async_to_sync(self.download_package_file_async(file_name=file_name, token=token))

    async def download_package_file_async(self, file_name: str, token: str) -> None:
        await self.download_package_file_stream_async(token, file_name)

    def download_package_files_sync(self, destination_directory: str, files: list) -> None:
        """Download all content set package files (sync version)"""
        return _async_to_sync(self.download_package_files_async(
            destination_directory=destination_directory,
            files=files
        ))

    async def download_package_files_async(self, destination_directory: str, files: list) -> None:
        """Download all content set package files (async version)"""
        LOGGER.info("Downloading files to %s ...", destination_directory)

        params = [(Path(destination_directory) / f["fileName"], f["token"]) for f in files]
        await _pool_tasks(self.download_package_file_async, params)

    # Downloading resource files

    def download_file_sync(self, url_structure: tuple[str, str]) -> None:
        """Download resource file (sync version)

        The url_structure parameter is a tuple (resource_directory_path, url)
        """
        (path, url) = url_structure
        return _async_to_sync(self.download_file_async(path=path, url=url))

    @_retry_async
    async def download_file_async(self, path: str, url: str) -> None:
        LOGGER.info("Downloading file %s ...", url)
        response = await self._async_client.get(url)
        response.raise_for_status()

        file_path = f"{path}/{os.path.basename(urlparse(url).path)}"

        with open(file_path, "wb") as f:
            f.write(response.content)

        LOGGER.info("Done downloading file %s", url)

    def get_resource_list(self, dest_folder: str) -> list[tuple[str, str]]:
        """Determine the URLs of downloadable resource files, based on already-downloaded package files"""
        arr_urls = []
        # Find *_resources_*.json file
        name_list = glob.glob(f"{dest_folder}/*_resources_*.json")
        if len(name_list) > 0:
            # Create parent resources folder
            os.makedirs(f"{dest_folder}/resources/", exist_ok=True)

            # Load the resource file - we may have several resources files, one per entity/sub-entity
            for res_file in name_list:
                entity_name = res_file.split("_")[0]
                resource_folder = f"{dest_folder}/resources/{entity_name}"
                os.makedirs(resource_folder, exist_ok=True)
                with open(res_file) as f:
                    data = json.load(f)
                    for u in data:
                        arr_urls.append((resource_folder, u["url"]))
        return arr_urls

    def extract_resources_sync(self, dest_folder: str) -> None:
        """Download all resource files (sync version)"""
        # download all binaries before expiration
        return _async_to_sync(self.extract_resources_async(dest_folder=dest_folder))

    async def extract_resources_async(self, dest_folder: str):
        LOGGER.info("Extracting resources to %s ...", dest_folder)
        await _pool_tasks(self.download_file_async, self.get_resource_list(dest_folder))
        LOGGER.info("Done extracting resources")

    # fetch() and fetch_sync() perform the entire process of fetching a content set to disk
    async def fetch_async(
        self,
        content_set_name: str,
        preset_content_set_name: str,
        internal_change_number: int = 0,
        output_format: Literal["json", "parquet"] = "json",
        split_files: bool = False,
        extract_resources: bool = False,
        token: Optional[str] = None,
    ) -> str:
        # request package
        if not token:
            token = await self.request_package_async(
                content_set_name,
                preset_content_set_name,
                internal_change_number,
                output_format,
                split_files,
            )

        # create the destination folder
        dest_folder = f"{config.OUT_DIR}/{content_set_name}_{token}"
        os.makedirs(dest_folder, exist_ok=True)

        # get the list of files to be downloaded
        status, files = await self.get_files_async(token)

        if files is not None:
            # Step 3.1 - download all the generated files
            await self.download_package_files_async(dest_folder, files)

            # Step 3.2 - download resources if requested
            if extract_resources:
                await self.extract_resources_async(dest_folder)
        else:
            LOGGER.error("FAILURE: Download packaged files: no file for download found!")
        return dest_folder

    def fetch_sync(
        self,
        content_set_name: str,
        preset_content_set_name: str,
        internal_change_number: int = 0,
        output_format: Literal["json", "parquet"] = "json",
        split_files: bool = False,
        extract_resources: bool = False,
        token: Optional[str] = None,
    ) -> str:
        """
        Fetch a content set by name, producing files on disc.

        This is our 'do-everything' function; most users of the
        library will want to use only this function or its async
        equivalent.
        """
        return _async_to_sync(
            self.fetch_async(
                content_set_name=content_set_name,
                preset_content_set_name=preset_content_set_name,
                internal_change_number=internal_change_number,
                output_format=output_format,
                split_files=split_files,
                extract_resources=extract_resources,
                token=token
            )
        )
    fetch = fetch_sync

    @_retry_async
    async def download_package_file_stream_async(self, token: str, file_name: str) -> None:
        """Download the file via stream, and resumed if it is not completed due to some issue.

        (async version)"""
        LOGGER.info("Downloading file %s ...", file_name)

        url, headers = self.prepare_request_stream(token, file_name)
        async with self._async_client.stream(
            "GET", url, headers=headers, follow_redirects=True
        ) as response:
            continue_download = self.handle_response_stream(file_name, response)
            if not continue_download:
                return
            with open(file_name, "ab") as file:
                async for chunk in response.aiter_bytes(config.CHUNK_SIZE):
                    file.write(chunk)
        LOGGER.info("Done downloading file %s", file_name)

    def download_package_file_stream_sync(self, token: str, file_name: str) -> None:
        """Download the file via stream, and resumed if it is not completed due to some issue.

        (sync version)"""
        return _async_to_sync(self.download_package_file_stream_async(token=token, file_name=file_name))

    def handle_response_stream(self, file_name: str, response: Response) -> bool:
        if response.status_code == 206:
            LOGGER.info("Resuming the download for '%s'", file_name)
            return True

        # If server returns "Range Not Satisfiable",
        # then we can conclude that file has been fully downloaded.
        if response.status_code == 416:
            LOGGER.info("File '%s' is already fully downloaded.", file_name)
            return False

        if response.status_code not in [200, 206]:
            response.raise_for_status()
        return True

    def prepare_request_stream(self, token: str, file_name: str) -> tuple[str, dict[str, str]]:
        current_chunk_length = get_file_length(file_name)
        url = f"{self.server_url}downloadPackage?token={token}"
        headers = self._setup_headers("binary", current_chunk_length)
        return url, headers


def _get_description(available_content_sets: list[dict[str, str]]) -> str:
    lines = ["\033[1mAVAILABLE CONTENTSETS\033[0m", " "]

    for cs in available_content_sets:
        lines.append("NAME:              " + cs.get("name", ""))
        lines.append("CONTENTSET:PRESET: " + cs["contentSet"] + ":" + cs["preset"])
        lines.append("DESCRIPTION:       " + cs.get("description", ""))
        lines.append(" ")

    return "\n".join(lines)


def _get_content_set_presets(available_content_sets: list[dict[str, str]]) -> set[str]:
    return {cs["contentSet"] + ":" + cs["preset"] for cs in available_content_sets}


def main():
    logging.basicConfig(
        level=logging.WARNING,
        format='%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        force=True
    )

    client = Client()
    print("Loading available content sets...")
    available_content_sets = client.get_content_sets_info_sync()

    description = _get_description(available_content_sets)
    cs_presets = _get_content_set_presets(available_content_sets)
    print()

    parser = argparse.ArgumentParser(
        description="Bulk download IP datasets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=description,
    )
    parser.add_argument(
        "--internal-change-number",
        dest="changenumber",
        default=0,
        type=int,
        help="Select only data newer than this internal change number (default: 0)",
    )
    parser.add_argument(
        "--format",
        dest="fmt",
        default="json",
        choices=["json", "parquet"],
        help="Format for the downloaded files, either json or parquet (default: json)",
    )
    parser.add_argument(
        "--split",
        dest="split",
        action="store_true",
        help="Split files"
    )
    parser.add_argument(
        "--extract",
        dest="extract",
        action="store_true",
        help="Download and extract resources (may be slow)",
    )
    parser.add_argument(
        "--verbose", "-v",
        dest="verbose",
        action="store_true",
        help="Make the operation more talkative",
    )
    parser.add_argument(
        "--token",
        dest="token",
        default=None,
        type=str,
        help="Proceed with existing token",
    )
    parser.add_argument(
        "contentset",
        metavar="CONTENTSET:PRESET",
        nargs="+",
        help="The name of the contentset and its preset",
    )
    args = parser.parse_args()

    if args.verbose:
        LOGGER.setLevel(logging.INFO)

    for csp in args.contentset:
        if csp not in cs_presets:
            sys.stderr.write(
                f"Error: argument CONTENTSET:PRESET: invalid parameter: '{csp}' (please refer to the help)"
            )
            sys.exit(1)

    cspairs = [x.split(":") for x in args.contentset]
    for cs in cspairs:
        start = time.time()
        print(f"Process started at {datetime.now().isoformat(sep=' ', timespec='milliseconds')}...")

        try:
            client.fetch(
                content_set_name=cs[0],
                preset_content_set_name=cs[1],
                internal_change_number=args.changenumber,
                output_format=args.fmt,
                split_files=args.split,
                extract_resources=args.extract,
                token=args.token
            )
        except Exception as e:
            sys.stderr.write("ERROR\n")
            if isinstance(e, MissingAuth):
                sys.stderr.write(f"{e}\n")
            elif isinstance(e, HTTPResponseNotOK):
                sys.stderr.write(f"{e.message}\n")
            else:
                sys.stderr.write(
                    "An unexpected error occurred. Please report the details below to your Clarivate contact.\n\n"
                )
                sys.stderr.write(f"parameters={sys.argv}")
                sys.stderr.write(traceback.format_exc())
            sys.exit(1)

        elapsed = (time.time() - start) / 60
        time.sleep(1)  # for logs to be flushed
        print(f"Process completed! Time taken: {elapsed:.3f} minutes")


if __name__ == "__main__":
    main()
