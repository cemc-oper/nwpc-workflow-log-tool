import os
from nwpc_work_flow_model.sms.node_variable import SmsNodeVariable, SmsNodeVariableType


class TestSmsNodeVariable(object):
    def test_create(self):
        var_name = 'var_name'
        var_type = SmsNodeVariableType.Variable
        var_value = 'var value'
        var = SmsNodeVariable(var_type, var_name, var_value)
        assert var.name == var_name
        assert var.variable_type == var_type
        assert var.value == var_value

    def test_to_dict(self):
        var_name = 'var_name'
        var_type = SmsNodeVariableType.Variable
        var_value = 'var value'
        var = SmsNodeVariable(var_type, var_name, var_value)
        var_dict = var.to_dict()
        assert var_dict['name'] == var_name
        assert var_dict['variable_type'] == var_type
        assert var_dict['value'] == var_value

    def test_create_from_dict(self):
        var_name = 'var_name'
        var_type = SmsNodeVariableType.Variable
        var_value = 'var value'
        var_dict = {
            'name': var_name,
            'variable_type': var_type,
            'value': var_value
        }
        var = SmsNodeVariable.create_from_dict(var_dict)

        assert var.name == var_name
        assert var.variable_type == var_type
        assert var.value == var_value
