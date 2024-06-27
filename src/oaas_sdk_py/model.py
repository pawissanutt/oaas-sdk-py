

class OaasObjectOrigin:
    def __init__(self, json_dict):
        self.json_dict = json_dict

    @property
    def args(self):
        if 'args' not in self.json_dict or self.json_dict['args'] is None:
            self.json_dict['args'] = []
        return self.json_dict['args']

    @property
    def func(self):
        return self.json_dict['funcName']


class OaasObject:

    def __init__(self, json_dict):
        self.json_dict = json_dict
        self.data = self.json_dict.get("data", {})
        if self.data is None:
            self.data = {}
        self.updated_keys: [str] = []
        self.meta = Metadata(self.json_dict.get("_meta", {}))

    @property
    def id(self):
        return self.meta.id

    @property
    def cls(self):
        return self.meta.cls


class Metadata:
    def __init__(self, json_dict):
        self.json_dict = json_dict
        self.id = self.json_dict.get("id")
        self.cls = self.json_dict.get("cls")
        self.ver_ids = self.json_dict.get("verIds")
        self.refs = self.json_dict.get("refs")


class OaasTask:
    input: [OaasObject]
    output_obj: OaasObject = None

    def __init__(self, json_dict):
        self.json_dict = json_dict
        if 'output' in json_dict:
            self.output_obj = OaasObject(json_dict['output'])
        self.main_obj = OaasObject(json_dict['main'])
        self.alloc_url = json_dict.get('allocOutputUrl', {})
        self.alloc_main_url = json_dict.get('allocMainUrl', {})
        if 'inputs' in json_dict:
            self.inputs = [OaasObject(input_dict) for input_dict in json_dict['inputs']]
        else:
            self.inputs = []
        self.main_get_keys = json_dict.get('mainGetKeys', {})
        self.main_put_keys = json_dict.get('mainPutKeys', {})
        self.output_keys = json_dict.get('outputKeys', {})
        self.input_keys = json_dict.get('inputKeys', [])
        self.req_body = json_dict.get('reqBody', {})
        if 'args' in json_dict:
            self.args = json_dict['args']
        else:
            self.args = {}

    @property
    def id(self):
        return self.json_dict['id']

    @property
    def func(self):
        return self.json_dict['funcKey']

    @property
    def immutable(self) -> bool:
        return self.json_dict.get("immutable", False)
