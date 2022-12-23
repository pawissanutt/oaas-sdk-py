

class OaasObjectOrigin:
    def __init__(self, json_dict):
        self.json_dict = json_dict

    @property
    def args(self):
        if 'args' not in self.json_dict or self.json_dict['args'] is None:
            self.json_dict['args'] = {}
        return self.json_dict['args']

    @property
    def func(self):
        return self.json_dict['funcName']


class OaasObject:
    def __init__(self, json_dict):
        self.json_dict = json_dict
        self.origin = OaasObjectOrigin(self.json_dict["origin"])

    @property
    def id(self):
        return self.json_dict["id"]

    @property
    def record(self):
        return self.json_dict.get("data", {})

    @property
    def data(self):
        return self.json_dict.get("data", {})


class OaasTask:

    def __init__(self, json_dict):
        self.json_dict = json_dict
        self.output_obj = OaasObject(json_dict['output'])
        self.main_obj = OaasObject(json_dict['main'])
        self.alloc_url = json_dict.get('allocOutputUrl', {})
        if 'inputs' in json_dict:
            self.inputs = [OaasObject(input_dict) for input_dict in json_dict['inputs']]
        else:
            self.inputs = []
        self.main_keys = json_dict.get('mainKeys', {})
        self.input_keys = json_dict.get('inputKeys', [])
    @property
    def args(self):
        return self.output_obj.origin.args

    @property
    def id(self):
        return self.output_obj.id