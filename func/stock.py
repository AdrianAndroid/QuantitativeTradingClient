class Stock:
    # _dict = {
    #     '_code': '',
    #     '_name': '',
    #     '_type': ''
    # }

    def __init__(self, _code, _name, _type):
        self._code = _code
        self._name = _name
        self._type = _type

    def set_name(self, _name):
        self._name = _name

    def set_code(self, _code):
        self._code = _code

    def set_type(self, _type):
        self._type = _type

    def read_code(self):
        return self._code

    def read_name(self):
        return self._name

    def read_type(self):
        return self._type

    def read_type_code(self):
        return f'{self.read_type()}{self.read_code()}'

    def read_type_code_name(self):
        return f'{self.read_type()}{self.read_code()}{self.read_name()}'

    def contains_S_or_T(self):
        return not self._name or 'S' in self._name or 'T' in self._name

    def __eq__(self, other):
        if not isinstance(other, Stock):
            return False
        return (self._code == other._code and
                self._name == other._name and
                self._type == other._type)

    def __hash__(self):
        return hash((self._code, self._name, self._type))

    def __repr__(self):
        return f"Stock(code='{self._code}', name='{self._name}', type='{self._type}')"
