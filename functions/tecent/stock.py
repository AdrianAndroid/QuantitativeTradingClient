class Stock:
    _dict = {
        '_code': '',
        '_name': '',
        '_type': ''
    }

    def __init__(self, _code, _name, _type):
        self.set_code(_code)
        self.set_name(_name)
        self.set_type(_type)

    def set_name(self, _name):
        self._dict['_name'] = _name

    def set_code(self, _code):
        self._dict['_code'] = _code

    def set_type(self, _type):
        self._dict['_type'] = _type

    def read_code(self):
        return self._dict['_code']

    def read_name(self):
        return self._dict['_name']

    def read_type(self):
        return self._dict['_type']
