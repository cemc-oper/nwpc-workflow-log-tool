from .node_variable import SmsNodeVariable, SmsNodeVariableType
from .node_type import NodeType


class SmsNode(object):
    def __init__(self):
        self.variable_list = []
        self.generated_variable_list = []

        """
        inherited_variable_list, an array of objects as follows:
            {
                'path': node_path,
                'variable_list': variable list,
                'generated_variable_list': generated variable list
            }
        """
        self.inherited_variable_list = []

        self.name = ''
        self.path = None
        self.status = None
        self.node_type = None

    @staticmethod
    def create_from_cdp_show_output(cdp_output):
        # find status
        cur_line_no = 0
        line_count = len(cdp_output)

        while cur_line_no < line_count:
            line = cdp_output[cur_line_no]
            if len(line) > 0 and line.startswith('/') and not line.startswith('/-CDP'):
                break
            cur_line_no += 1
        if cur_line_no == line_count:
            return None

        # find show
        while cur_line_no < line_count:
            line = cdp_output[cur_line_no]
            if line == '\n':
                break
            cur_line_no += 1

        cur_line_no += 1
        if cur_line_no >= line_count:
            return None

        line = cdp_output[cur_line_no]
        if line[0].startswith('#'):
            return None

        start_line_no = cur_line_no

        cur_line_no += 1
        end_line_no = -1
        while cur_line_no < line_count:
            line = cdp_output[cur_line_no]
            if not line.startswith('  '):
                end_line_no = cur_line_no
                break
            cur_line_no += 1

        if end_line_no == -1:
            return None

        # get variables
        node_line = cdp_output[start_line_no: end_line_no]
        node = SmsNode()
        tokens = node_line[0].strip().split(' ')
        if len(tokens) > 5:
            node.node_type = tokens[0]
            node.name = tokens[1]
            node.status = tokens[4]

        for line in node_line[1:]:
            line = line.strip()
            if line.startswith('# genvar '):
                index = line.find(' ', 9)
                if index == -1:
                    continue
                variable_name = line[9:index]
                variable_value = line[index + 1:]
                if variable_value[0] == '\'' and variable_value[-1] == '\'':
                    variable_value = variable_value[1:-1]
                variable = SmsNodeVariable(
                    SmsNodeVariableType.GeneratedVariable,
                    variable_name,
                    variable_value
                )
                node.generated_variable_list.append(variable)
            elif line.startswith('edit '):
                index = line.find(' ', 5)
                if index == -1:
                    continue
                variable_name = line[5:index]
                variable_value = line[index + 1:]
                if variable_value[0] == '\'' and variable_value[-1] == '\'':
                    variable_value = variable_value[1:-1]
                variable = SmsNodeVariable(
                    SmsNodeVariableType.Variable,
                    variable_name,
                    variable_value
                )
                node.variable_list.append(variable)
            else:
                pass

        return node

    @staticmethod
    def create_from_cdp_info_output(cdp_output):
        # find status
        cur_line_no = 0
        line_count = len(cdp_output)

        while cur_line_no < line_count:
            line = cdp_output[cur_line_no]
            if len(line) > 0 and line.startswith('//'):
                break
            cur_line_no += 1
        if cur_line_no == line_count:
            return None

        node = SmsNode()

        path_line = cdp_output[cur_line_no].strip()
        path_line_tokens = path_line.split(' ')
        node_path = path_line_tokens[0]
        node.path = node_path[node_path.index('/', 2):]
        node.name = node_path[node_path.rfind('/')+1:]
        node.node_type = path_line_tokens[1][1:-1]
        node.status = path_line_tokens[-1]

        cur_line_no += 1
        while cur_line_no < line_count:
            line = cdp_output[cur_line_no]
            if line.startswith('Variables'):
                break
            cur_line_no += 1

        if cur_line_no == line_count:
            return None

        variable_start_line_no = cur_line_no = cur_line_no + 1

        while cur_line_no < line_count:
            line = cdp_output[cur_line_no]
            if not line.startswith('  '):
                break
            cur_line_no += 1
        variable_end_line_no = cur_line_no

        def get_variable_from_variable_line(variable_line, path_start):
            name_start = 0
            value_end = path_start-2
            if variable_line[0] == '(':
                variable_type = SmsNodeVariableType.GeneratedVariable
                name_start += 1
            else:
                variable_type = SmsNodeVariableType.Variable

            equal_index = variable_line.index('=')
            var_name = variable_line[name_start:equal_index].strip()
            var_value = variable_line[equal_index+2:value_end].strip()

            variable = SmsNodeVariable(name=var_name, value=var_value, variable_type=variable_type)
            return variable

        for a_variable_line in cdp_output[variable_start_line_no: variable_end_line_no]:
            a_variable_line = a_variable_line.strip()
            path_start = a_variable_line.rindex('[')
            node_path = a_variable_line[path_start+1:-1]
            if len(node_path) == 0:
                variable = get_variable_from_variable_line(a_variable_line, path_start)
                if variable.variable_type == SmsNodeVariableType.Variable:
                    node.variable_list.append(variable)
                else:
                    node.generated_variable_list.append(variable)
            else:
                if not (len(node.inherited_variable_list) > 0 and
                        node.inherited_variable_list[-1]['path'] == node_path):
                    node.inherited_variable_list.append({
                        'path': node_path,
                        'variable_list': [],
                        'generated_variable_list': []
                    })

                variable = get_variable_from_variable_line(a_variable_line, path_start)
                if variable.variable_type == SmsNodeVariableType.Variable:
                    node.inherited_variable_list[-1]['variable_list'].append(variable)
                else:
                    node.inherited_variable_list[-1]['generated_variable_list'].append(variable)

        return node

    def to_dict(self):
        inherited_variable_list = []
        for node in self.inherited_variable_list:
            inherited_variable_list.append({
                'path': node['path'],
                'variable_list': [var.to_dict() for var in node['variable_list']],
                'generated_variable_list': [var.to_dict() for var in node['generated_variable_list']]
            })
        return {
            'name': self.name,
            'status': self.status,
            'node_type': self.node_type,
            'variable_list': [var.to_dict() for var in self.variable_list],
            'generated_variable_list': [var.to_dict() for var in self.generated_variable_list],
            'inherited_variable_list': inherited_variable_list
        }

    @staticmethod
    def create_from_dict(node_dict):
        node = SmsNode()
        node.name = node_dict['name']
        node.status = node_dict['status']
        node.node_type = node_dict['node_type']
        for a_var_dict in node_dict['variable_list']:
            a_var = SmsNodeVariable.create_from_dict(a_var_dict)
            node.variable_list.append(a_var)
        for a_var_dict in node_dict['generated_variable_list']:
            a_var = SmsNodeVariable.create_from_dict(a_var_dict)
            node.generated_variable_list.append(a_var)

        for a_parent in node_dict['inherited_variable_list']:
            node.inherited_variable_list.append({
                'path': a_parent['path'],
                'variable_list': [SmsNodeVariable.create_from_dict(a_var_dict)
                                  for a_var_dict in a_parent['variable_list']],
                'generated_variable_list': [SmsNodeVariable.create_from_dict(a_var_dict)
                                  for a_var_dict in a_parent['generated_variable_list']]
            })

        return node

    def get_variable(self, variable_name):
        for var in self.variable_list:
            if var.name == variable_name:
                return var
        for var in self.generated_variable_list:
            if var.name == variable_name:
                return var

        for node in self.inherited_variable_list:
            for var in node['variable_list']:
                if var.name == variable_name:
                    return var
            for var in node['generated_variable_list']:
                if var.name == variable_name:
                    return var
        return None

    def get_variable_value(self, variable_name):
        var = self.get_variable(variable_name)
        if var is None:
            return None
        else:
            return var.value
