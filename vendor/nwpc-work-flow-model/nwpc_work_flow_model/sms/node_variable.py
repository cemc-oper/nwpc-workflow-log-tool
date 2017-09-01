class SmsNodeVariableType(object):
    Variable = 'var'
    GeneratedVariable = 'genvar'


class SmsNodeVariable(object):
    def __init__(self, variable_type, name, value):
        self.name = name
        self.variable_type = variable_type
        self.value = value

    def to_dict(self):
        return {
            'name': self.name,
            'variable_type': self.variable_type,
            'value': self.value
        }

    @staticmethod
    def create_from_dict(var_dict):
        var = SmsNodeVariable(var_dict['variable_type'], var_dict['name'], var_dict['value'])
        return var
