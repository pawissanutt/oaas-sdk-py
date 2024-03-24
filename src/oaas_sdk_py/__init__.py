import asyncio
import json
from abc import abstractmethod
from typing import Dict, List, Any

import aiofiles
import aiofiles.os
import aiohttp
from aiohttp import ClientSession, ClientResponse

from .model import *


class OaasException(BaseException):
    pass


async def _load_file(session: ClientSession,
                     url: str) -> ClientResponse:
    resp = await session.get(url)
    if not resp.ok:
        raise OaasException(f"Got error when get the data from S3 (code:{resp.status})")
    return resp


async def _allocate(session: ClientSession,
                    url):
    resp = await session.get(url)
    if not resp.ok:
        raise OaasException(f"Got error when allocate keys (code:{resp.status})")
    return await resp.json()


async def _upload(session: ClientSession,
                  path,
                  url):
    size = await aiofiles.os.path.getsize(path)
    with open(path, "rb") as f:
        headers = {"content-length": str(size)}
        resp = await session.put(url, headers=headers, data=f)
        if not resp.ok:
            raise OaasException(f"Got error when put the data to S3 (code:{resp.status})")


class OaasInvocationCtx:
    success: bool = True
    error: str = None
    extensions: dict[str, str] = None
    allocate_url_dict = None
    allocate_main_url_dict = None
    resp_body: dict[str, Any] = None

    def __init__(self, json_dict: Dict):
        self.json_dict = json_dict
        self.task = OaasTask(json_dict)

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
                               data: bytes) -> None:
        if self.allocate_url_dict is None:
            await self.allocate_file(session)
        url = self.allocate_url_dict[key]
        if url is None:
            raise OaasException(f"The output object not accept '{key}' as key")
        resp = await session.put(url, data=data)
        if not resp.ok:
            raise OaasException("Got error when put the data to S3")
        self.task.output_obj.updated_keys.append(key)

    async def upload_main_byte_data(self,
                                    session: ClientSession,
                                    key: str,
                                    data: bytes) -> None:
        if self.allocate_main_url_dict is None:
            await self.allocate_main_file(session)
        url = self.allocate_main_url_dict[key]
        if url is None:
            raise OaasException(f"The main object not accept '{key}' as key")
        resp = await session.put(url, data=data)
        if not resp.ok:
            raise OaasException("Got error when put the data to S3")
        self.task.main_obj.updated_keys.append(key)

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

    async def load_main_file(self, session: ClientSession, key: str) -> ClientResponse:
        if key not in self.task.main_keys:
            raise OaasException(f"NO such key '{key}' in main object")
        return await _load_file(session, self.task.main_keys[key])

    async def load_input_file(self, session: ClientSession, input_index: int, key: str) -> ClientResponse:
        if input_index > len(self.task.input_keys):
            raise OaasException(f"Input index {input_index} out of range({len(self.task.input_keys)})")
        if key not in self.task.input_keys[input_index]:
            raise OaasException(f"No such key '{key}' in input object")
        return await _load_file(session, self.task.input_keys[input_index][key])

    def create_completion(self,
                          main_data: Dict = None,
                          output_data: Dict = None):
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
                output_update["data"] = self.task.output_obj.data
            else:
                output_update["data"] = output_data
            output_update["updatedKeys"] = self.task.output_obj.updated_keys

        return {
            'id': self.task.id,
            'success': self.success,
            'errorMsg': self.error,
            'main': main_update,
            'output': output_update,
            'extensions': self.extensions,
            'body': self.resp_body
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
    async def handle(self, ctx: OaasInvocationCtx):
        pass


class Router:
    _default_handler: Handler = None
    _handlers: Dict[str, Handler] = {}

    def __init__(self):
        pass

    def register(self, handler: Handler):
        self._default_handler = handler

    def register(self, fn_key: str, handler: Handler):
        self._handlers[fn_key] = handler

    async def handle_task(self, json_task):
        ctx = OaasInvocationCtx(json_task)
        if ctx.task.func in self._handlers:
            await self._handlers[ctx.task.func].handle(ctx)
            resp = ctx.create_completion()
            return resp
        elif self._default_handler is not None:
            await self._default_handler.handle(ctx)
            resp = ctx.create_completion()
            return resp
        else:
            return None
