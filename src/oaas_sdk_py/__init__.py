import asyncio
import json
from asyncio import StreamReader

import aiohttp


class OaasException(BaseException):
    pass


class OaasTask:

    def __init__(self, json_dict):
        self.json_dict = json_dict
        self.output_obj = json_dict['output']
        self.alloc_url = json_dict['allocOutputUrl']
        self.output_id = self.output_obj['id']
        self.main_id = json_dict['src']['id']
        self.inputs = json_dict['inputs']
        self.main_keys = json_dict['mainKeys']
        self.input_keys = json_dict['inputKeys']
        self.allocate_url_dict = None

    @property
    def args(self):
        return self.output_obj.get('origin', {}).get('args', {})

    def get_main_resource_url(self, key: str):
        return self.json_dict['mainKeys'][key]

    async def allocate_file(self) -> dict:
        async with aiohttp.ClientSession() as session:
            client_resp = await session.get(self.alloc_url)
            if not client_resp.ok:
                raise OaasException("Got error when allocate keys")
            resp_dict = await client_resp.json()
            if self.allocate_url_dict is None:
                self.allocate_url_dict = resp_dict
            else:
                self.allocate_url_dict = self.allocate_url_dict | resp_dict
            return self.allocate_url_dict

    async def allocate_collection(self, keys: list[str]) -> dict:
        async with aiohttp.ClientSession() as session:
            client_resp = await session.post(self.alloc_url, json=keys)
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
        if key not in self.main_keys:
            raise OaasException(f"NO such key '{key}' in main object")
        return await self.load_file(self.main_keys[key])

    async def load_input_file(self, input_index: int, key: str):
        if input_index > len(self.input_keys):
            raise OaasException(f"Input index {input_index} out of range({len(self.input_keys)})")
        if key not in self.input_keys[input_index]:
            raise OaasException(f"No such key '{key}' in input object")
        return await self.load_file(self.input_keys[input_index][key])

    async def load_file(self, url: str) -> StreamReader:
        async with aiohttp.ClientSession() as session:
            resp = await session.get(url)
            if not resp.ok:
                raise OaasException("Got error when get the data to S3")
            return resp.content

    def create_completion(self,
                          success: bool = True,
                          error: str = None,
                          record: dict = None,
                          extensions: dict = None):
        return {
            'id': self.output_id,
            'success': success,
            'errorMsg': error,
            'embeddedRecord': record,
            'extensions': extensions
        }

    def create_reply_header(self, headers: dict = None):
        if headers is None:
            headers = {}
        headers["Ce-Id"] = str(self.output_id)
        headers["Ce-specversion"] = "1.0"
        headers["Ce-Source"] = "oaas/" + self.output_obj["origin"]["funcName"]
        headers["Ce-Type"] = "oaas.task.result"
        return headers


def parse_task_from_string(json_string: str) -> OaasTask:
    task = OaasTask(json.loads(json_string))
    return task


def parse_task_from_dict(json_dict: dict) -> OaasTask:
    return OaasTask(json_dict)
