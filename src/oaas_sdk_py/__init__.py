import asyncio
import json
from abc import abstractmethod
from asyncio import StreamReader
from typing import Dict, List
import aiofiles.os
import aiofiles
import aiohttp
from aiohttp import ClientSession

from .model import *


class OaasException(BaseException):
    pass


async def load_file(url: str) -> StreamReader:
    async with aiohttp.ClientSession() as session:
        resp = await session.get(url)
        if not resp.ok:
            raise OaasException("Got error when get the data to S3")
        return resp.content


async def _allocate(session: ClientSession,
                    url):
    client_resp = await session.get(url)
    if not client_resp.ok:
        raise OaasException("Got error when allocate keys")
    return await client_resp.json()


async def _upload(session: ClientSession,
                   path,
                   url):
    async with aiofiles.open(path, "rb") as f:
        size = await aiofiles.os.path.getsize(path)
        headers = {"content-length": str(size)}
        resp = await session.put(url, headers=headers, data=f)
        if not resp.ok:
            raise OaasException("Got error when put the data to S3")


class OaasInvocationCtx:
    def __init__(self, json_dict: Dict):
        self.json_dict = json_dict
        self.task = OaasTask(json_dict)
        self.allocate_url_dict = None
        self.allocate_main_url_dict = None

    @property
    def args(self):
        return self.task.args

    @property
    def id(self):
        return self.task.id

    def get_main_resource_url(self, key: str):
        return self.json_dict['mainKeys'][key]

    async def allocate_file(self,
                            session: ClientSession) -> dict:
        resp_dict = await _allocate(session, self.task.alloc_url)
        if self.allocate_url_dict is None:
            self.allocate_url_dict = resp_dict
        else:
            self.allocate_url_dict = self.allocate_url_dict | resp_dict
        return self.allocate_url_dict

    async def allocate_main_file(self,
                                 session: ClientSession) -> dict:
        resp_dict = await _allocate(session, self.task.alloc_main_url)
        if self.allocate_main_url_dict is None:
            self.allocate_main_url_dict = resp_dict
        else:
            self.allocate_main_url_dict = self.allocate_main_url_dict | resp_dict
        return self.allocate_main_url_dict

    async def allocate_collection(self,
                                  session: ClientSession,
                                  keys: List[str]) -> Dict[str, str]:
        client_resp = await session.post(self.task.alloc_url, json=keys)
        if not client_resp.ok:
            raise OaasException("Got error when allocate keys")
        resp_dict = await client_resp.json()
        if self.allocate_url_dict is None:
            self.allocate_url_dict = resp_dict
        else:
            self.allocate_url_dict = self.allocate_url_dict | resp_dict
        return self.allocate_url_dict

    async def upload_byte_data(self,
                               session: ClientSession,
                               key: str,
                               data: bytearray) -> None:
        if self.allocate_url_dict is None:
            await self.allocate_file(session)
        url = self.allocate_url_dict[key]
        if url is None:
            raise OaasException(f"The output object not accept '{key}' as key")
        resp = await session.put(url, data=data)
        if not resp.ok:
            raise OaasException("Got error when put the data to S3")

    async def upload_file(self,
                          session: ClientSession,
                          key: str,
                          path: str) -> None:
        if self.allocate_url_dict is None:
            await self.allocate_file(session)
        url = self.allocate_url_dict[key]
        if url is None:
            raise OaasException(f"The output object not accept '{key}' as key")
        self.task.output_obj.updated_keys.append(key)
        await _upload(session, path, url)

    async def upload_main_file(self,
                               session: ClientSession,
                               key: str,
                               path: str) -> None:
        if self.allocate_main_url_dict is None:
            await self.allocate_main_file(session)
        url = self.allocate_main_file[key]
        if url is None:
            raise OaasException(f"The main object not accept '{key}' as key")
        self.task.main_obj.updated_keys.append(key)
        await _upload(session, path, url)

    async def upload_collection(self,
                                session: ClientSession,
                                key_to_file: Dict[str, str]) -> None:
        await self.allocate_collection(session, list(key_to_file.keys()))
        promise_list = [self.upload_file(session, k, v) for k, v in key_to_file.items()]
        self.task.output_obj.updated_keys.extend(list(key_to_file.keys()))
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
                          main_data: Dict = None,
                          output_data: Dict = None,
                          extensions: Dict = None):
        main_update = {}
        if not self.task.immutable:
            if main_data is None:
                main_update["data"] = self.task.main_obj.data
            else:
                main_update["data"] = main_data
            main_update["updatedKeys"] = self.task.main_obj.updated_keys

        output_update = {}
        if self.task.output_obj is not None:
            if output_data is None:
                output_update["data"] = self.task.main_obj.data
            else:
                output_update["data"] = main_data
            output_update["updatedKeys"] = self.task.output_obj.updated_keys

        return {
            'id': self.task.id,
            'success': success,
            'errorMsg': error,
            'main': main_update,
            'output': output_update,
            'extensions': extensions
        }

    def create_reply_header(self, headers=None):
        if headers is None:
            headers = {}
        headers["Ce-Id"] = str(self.task.id)
        headers["Ce-specversion"] = "1.0"
        if self.task.output_obj is not None:
            headers["Ce-Source"] = "oaas/" + self.task.func
        headers["Ce-Type"] = "oaas.task.result"
        return headers


def parse_ctx_from_string(json_string: str) -> OaasInvocationCtx:
    return OaasInvocationCtx(json.loads(json_string))


def parse_ctx_from_dict(json_dict: dict) -> OaasInvocationCtx:
    return OaasInvocationCtx(json_dict)


class Handler:
    @abstractmethod
    def handle(self, ctx: OaasInvocationCtx):
        pass


class Router:
    _handlers: Dict[str, Handler] = {}

    def __init__(self):
        pass

    def register(self, fn_key: str, handler: Handler):
        self._handlers[fn_key] = handler

    def handle_task(self, json_task):
        ctx = OaasInvocationCtx(json_task)
        if ctx.task.func in self._handlers:
            resp = self._handlers[ctx.task.func].handle(ctx)
            return resp
        else:
            return None
