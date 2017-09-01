import os
from nwpc_work_flow_model.sms.sms_node import SmsNode
from nwpc_work_flow_model.sms.node_status import NodeStatus
from nwpc_work_flow_model.sms.node_type import NodeType
from nwpc_work_flow_model.sms.node_variable import SmsNodeVariableType, SmsNodeVariable


class TestSmsNode(object):
    @staticmethod
    def create_node():
        node = SmsNode()
        node.variable_list.extend([
            SmsNodeVariable(SmsNodeVariableType.Variable, 'self_var_1', 'self_var_1_value'),
            SmsNodeVariable(SmsNodeVariableType.Variable, 'var', 'var1'),
            SmsNodeVariable(SmsNodeVariableType.Variable, 'father_var_2', 'father_var_2_in_node'),
        ])

        node.generated_variable_list.extend([
            SmsNodeVariable(SmsNodeVariableType.GeneratedVariable, 'self_gen_var_1', 'self_gen_var_1_value'),
            SmsNodeVariable(SmsNodeVariableType.GeneratedVariable, 'var', 'var2')
        ])

        # root_node -> father_node -> node
        node.inherited_variable_list.extend([
            {
                'path': '/root/father',
                'variable_list': [
                    SmsNodeVariable(SmsNodeVariableType.Variable, 'father_var', 'father_var_value'),
                    SmsNodeVariable(SmsNodeVariableType.Variable, 'father_var_2', 'father_var_2_value'),
                    SmsNodeVariable(SmsNodeVariableType.Variable, 'root_var_2', 'root_var_2_value_in_father')
                ],
                'generated_variable_list': [
                    SmsNodeVariable(SmsNodeVariableType.Variable, 'father_gen_var', 'father_gen_var_value')
                ]
            },
            {
                'path': '/root',
                'variable_list': [
                    SmsNodeVariable(SmsNodeVariableType.Variable, 'root_var', 'root_var_value'),
                    SmsNodeVariable(SmsNodeVariableType.Variable, 'root_var_2', 'root_var_2_value')
                ],
                'generated_variable_list': [
                    SmsNodeVariable(SmsNodeVariableType.Variable, 'root_gen_var', 'root_gen_var_value')
                ]
            }
        ])
        return node

    def test_node_variable(self):
        node = TestSmsNode.create_node()

        # node's variables
        assert node.get_variable_value('self_var_1') == 'self_var_1_value'
        assert node.get_variable_value('self_gen_var_1') == 'self_gen_var_1_value'

        # Variable type overrides generated variable type.
        assert node.get_variable_value('var') == 'var1'
        assert node.get_variable('var').variable_type == SmsNodeVariableType.Variable

        # inherited variables
        assert node.get_variable_value('father_var') == 'father_var_value'
        assert node.get_variable_value('root_var') == 'root_var_value'

        assert node.get_variable_value('father_var_2') == 'father_var_2_in_node'
        assert node.get_variable_value('root_var_2') == 'root_var_2_value_in_father'

    def test_show_normal_suite_cdp_output(self):
        with open(os.path.dirname(__file__) + "/data/cdp/show/normal_suite_cdp.txt") as f:
            cdp_output = f.readlines()
            node = SmsNode.create_from_cdp_show_output(cdp_output)
            assert node.get_variable_value('SUITE') == 'grapes_meso_v4_1'
            assert node.get_variable_value('DATE') == '15.01.2017'
            assert node.get_variable_value('DAY') == 'sunday'
            assert node.get_variable_value('DD') == '15'

            assert node.get_variable_value('SMSOUT') == '/cma/g1/nwp/SMSOUT'
            assert node.get_variable_value('SMSHOME') == '/cma/g1/nwp/SMSOUT'
            assert node.get_variable_value('VERSION') == '_v4_1'

    def test_show_normal_family_cdp_output(self):
        with open(os.path.dirname(__file__) + "/data/cdp/show/normal_family_cdp.txt") as f:
            cdp_output = f.readlines()
            node = SmsNode.create_from_cdp_show_output(cdp_output)
            assert node.get_variable_value('FAMILY') == 'cold'
            assert node.get_variable_value('FAMILY1') == 'cold'

            assert node.get_variable_value('SMSINCLUDE') == '/cma/u/nwp/smsworks/def/grapes_meso/include'
            assert node.get_variable_value('SMSFILES') == '/cma/u/nwp/smsworks/def/grapes_meso/smsfiles'

    def test_show_error_login_cdp(self):
        with open(os.path.dirname(__file__) + "/data/cdp/show/error_login_cdp.txt") as f:
            cdp_output = f.readlines()
            node = SmsNode.create_from_cdp_show_output(cdp_output)
            assert node is None

    def test_show_error_node_path_cdp(self):
        with open(os.path.dirname(__file__) + "/data/cdp/show/error_node_path_cdp.txt") as f:
            cdp_output = f.readlines()
            node = SmsNode.create_from_cdp_show_output(cdp_output)
            assert node is None

    def test_show_error_sms_server_cdp(self):
        with open(os.path.dirname(__file__) + "/data/cdp/show/error_sms_server_cdp.txt") as f:
            cdp_output = f.readlines()
            node = SmsNode.create_from_cdp_show_output(cdp_output)
            assert node is None

    def test_to_dict(self):
        node = TestSmsNode.create_node()
        node_dict = node.to_dict()

        assert len(node_dict['variable_list']) == 3
        assert len(node_dict['generated_variable_list']) == 2
        assert len(node_dict['inherited_variable_list']) == 2

    def test_create_from_dict(self):
        node_dict = {
            'name': 'node name',
            'status': NodeStatus.Unknown,
            'node_type': NodeType.Suite,
            'variable_list': [
                {
                    'name': 'var name',
                    'variable_type': SmsNodeVariableType.Variable,
                    'value': 'var value'
                }
            ],
            'generated_variable_list': [
                {
                    'name': 'gen var name',
                    'variable_type': SmsNodeVariableType.GeneratedVariable,
                    'value': 'gen var value'
                }
            ],
            'inherited_variable_list': [
                {
                    'path': '/root/father',
                    'variable_list': [
                        {
                            'name': 'father var name',
                            'variable_type': SmsNodeVariableType.Variable,
                            'value': 'father var value'
                        }
                    ],
                    'generated_variable_list': [
                        {
                            'name': 'father gen var name',
                            'variable_type': SmsNodeVariableType.GeneratedVariable,
                            'value': 'father gen var value'
                        }
                    ]
                },
                {
                    'path': '/root',
                    'variable_list': [
                        {
                            'name': 'root var name',
                            'variable_type': SmsNodeVariableType.Variable,
                            'value': 'root var value'
                        }
                    ],
                    'generated_variable_list': [
                        {
                            'name': 'root gen var name',
                            'variable_type': SmsNodeVariableType.GeneratedVariable,
                            'value': 'root gen var value'
                        }
                    ]
                }
            ]
        }

        node = SmsNode.create_from_dict(node_dict)

        assert node.name == node_dict['name']
        assert node.status == node_dict['status']
        assert node.node_type == node_dict['node_type']
        assert len(node.variable_list) == 1
        assert len(node.generated_variable_list) == 1
        assert len(node.inherited_variable_list) == 2

    def test_info_normal_suite_cdp_output(self):
        with open(os.path.dirname(__file__) + "/data/cdp/info/info_normal_suite.txt") as f:
            cdp_output = f.readlines()
            node = SmsNode.create_from_cdp_info_output(cdp_output)
            assert node.path == '/grapes_meso_v4_1'
            assert node.name == 'grapes_meso_v4_1'
            assert node.node_type == 'suite'
            assert node.status == 'active'

            assert len(node.variable_list) == 3
            assert len(node.generated_variable_list) == 12

            assert node.get_variable_value('SUITE') == 'grapes_meso_v4_1'
            assert node.get_variable_value('DATE') == '21.05.2017'
            assert node.get_variable_value('DAY') == 'sunday'
            assert node.get_variable_value('DD') == '21'

            assert node.get_variable_value('SMSOUT') == '/cma/g1/nwp/SMSOUT'
            assert node.get_variable_value('SMSHOME') == '/cma/g1/nwp/SMSOUT'
            assert node.get_variable_value('VERSION') == '_v4_1'

    def test_info_normal_family_cdp_output(self):
        with open(os.path.dirname(__file__) + "/data/cdp/info/info_normal_family.txt") as f:
            cdp_output = f.readlines()
            node = SmsNode.create_from_cdp_info_output(cdp_output)
            assert node.path == '/grapes_meso_v4_1/cold/00'
            assert node.name == '00'
            assert node.node_type == 'family'
            assert node.status == 'complete'

            assert node.get_variable_value('HH') == '00'

            assert node.get_variable_value('SUITE') == 'grapes_meso_v4_1'
            assert node.get_variable_value('DATE') == '21.05.2017'
            assert node.get_variable_value('DAY') == 'sunday'
            assert node.get_variable_value('DD') == '21'

            assert node.get_variable_value('SMSOUT') == '/cma/g1/nwp/SMSOUT'
            assert node.get_variable_value('SMSHOME') == '/cma/g1/nwp/SMSOUT'
            assert node.get_variable_value('VERSION') == '_v4_1'
