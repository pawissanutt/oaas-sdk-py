import asyncio
import json
from asyncio import StreamReader
from typing import Dict, List

import aiohttp

from .model import *


class OaasException(BaseException):
    pass


async def load_file(url: str) -> StreamReader:
    async with aiohttp.ClientSession() as session:
        resp = await session.get(url)
        if not resp.ok:
            raise OaasException("Got error when get the data to S3")
        return resp.content


class OaasInvocationCtx:
    def __init__(self, json_dict: Dict):
        self.json_dict = json_dict
        self.task = OaasTask(json_dict)
        self.allocate_url_dict = None

    @property
    def args(self):
        return self.task.args

    @property
    def id(self):
        return self.task.id

    def get_main_resource_url(self, key: str):
        return self.json_dict['mainKeys'][key]

    async def allocate_file(self) -> dict:
        async with aiohttp.ClientSession() as session:
            client_resp = await session.get(self.task.alloc_url)
            if not client_resp.ok:
                raise OaasException("Got error when allocate keys")
            resp_dict = await client_resp.json()
            if self.allocate_url_dict is None:
                self.allocate_url_dict = resp_dict
            else:
                self.allocate_url_dict = self.allocate_url_dict | resp_dict
            return self.allocate_url_dict

    async def allocate_collection(self, keys: List[str]) -> Dict[str, str]:
        async with aiohttp.ClientSession() as session:
            client_resp = await session.post(self.task.alloc_url, json=keys)
            if not client_resp.ok:
                raise OaasException("Got error when allocate keys")
            resp_dict = await client_resp.json()
            if self.allocate_url_dict is None:
                self.allocate_url_dict = resp_dict
            else:
                self.allocate_url_dict = self.allocate_url_dict | resp_dict
            return self.allocate_url_dict

    async def upload_byte_data(self, key: str, data: bytearray) -> None:
        if self.allocate_url_dict is None:
            await self.allocate_file()
        url = self.allocate_url_dict[key]
        if url is None:
            raise OaasException(f"The output object not accept '{key}' as key")
        async with aiohttp.ClientSession() as session:
            resp = await session.put(url, data=data)
            if not resp.ok:
                raise OaasException("Got error when put the data to S3")

    async def upload_file(self, key: str, path: str) -> None:
        if self.allocate_url_dict is None:
            await self.allocate_file()
        url = self.allocate_url_dict[key]
        if url is None:
            raise OaasException(f"The output object not accept '{key}' as key")
        with open(path, "rb") as f:
            async with aiohttp.ClientSession() as session:
                resp = await session.put(url, data=f)
                if not resp.ok:
                    raise OaasException("Got error when put the data to S3")

    async def upload_collection(self, key_to_file: dict[str, str]) -> None:
        await self.allocate_collection(list(key_to_file.keys()))
        promise_list = [self.upload_file(k, v) for k, v in key_to_file]
        await asyncio.gather(*promise_list)

    async def load_main_file(self, key: str) -> StreamReader:
        if key not in self.task.main_keys:
            raise OaasException(f"NO such key '{key}' in main object")
        return await load_file(self.task.main_keys[key])

    async def load_input_file(self, input_index: int, key: str):
        if input_index > len(self.task.input_keys):
            raise OaasException(f"Input index {input_index} out of range({len(self.task.input_keys)})")
        if key not in self.task.input_keys[input_index]:
            raise OaasException(f"No such key '{key}' in input object")
        return await load_file(self.task.input_keys[input_index][key])

    def create_completion(self,
                          success: bool = True,
                          error: str = None,
                          record: Dict = None,
                          extensions: Dict = None):
        return {
            'id': self.task.output_obj.id,
            'success': success,
            'errorMsg': error,
            'embeddedRecord': record,
            'extensions': extensions
        }

    def create_reply_header(self, headers=None):
        if headers is None:
            headers = {}
        headers["Ce-Id"] = str(self.task.output_obj.id)
        headers["Ce-specversion"] = "1.0"
        headers["Ce-Source"] = "oaas/" + self.task.output_obj.origin.func
        headers["Ce-Type"] = "oaas.task.result"
        return headers


def parse_ctx_from_string(json_string: str) -> OaasInvocationCtx:
    return OaasInvocationCtx(json.loads(json_string))


def parse_ctx_from_dict(json_dict: dict) -> OaasInvocationCtx:
    return OaasInvocationCtx(json_dict)
