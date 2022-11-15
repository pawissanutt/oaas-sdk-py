import json
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
            self.allocate_url_dict = await client_resp.json()
            return self.allocate_url_dict

    async def save_byte_data(self, key: str, data: bytearray) -> None:
        if self.allocate_url_dict is None:
            await self.allocate_file()
        url = self.allocate_url_dict[key]
        if url is None:
            raise OaasException(f"The output object not accept '{key}' as key")
        async with aiohttp.ClientSession() as session:
            resp = await session.put(url, data=data)
            if not resp.ok:
                raise OaasException("Got error when put the data to S3")


def parse_task(json_string) -> OaasTask:
    task = OaasTask(json.loads(json_string))
    return task
