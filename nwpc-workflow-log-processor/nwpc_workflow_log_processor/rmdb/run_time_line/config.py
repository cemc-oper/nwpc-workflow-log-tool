# coding: utf-8
import yaml


def load_processor_config(config_file_path):
    with open(config_file_path, 'r') as f:
        config = yaml.load(f)
        f.close()
        config['_file_path'] = config_file_path
        return config
