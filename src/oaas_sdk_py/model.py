

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
    updated_keys: [str] = []

    def __init__(self, json_dict):
        self.json_dict = json_dict
        self.origin = OaasObjectOrigin(self.json_dict["origin"])
        self.data = self.json_dict.get("data", {})

    @property
    def id(self):
        return self.json_dict["id"]


class OaasTask:
    input: [OaasObject] = []
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
        self.main_keys = json_dict.get('mainKeys', {})
        self.input_keys = json_dict.get('inputKeys', [])
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
        return self.json_dict["immutable"]
