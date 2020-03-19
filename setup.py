# coding: utf-8
from setuptools import setup, find_packages
import codecs
from os import path
import io
import re

with io.open("nwpc_workflow_log_tool/__init__.py", "rt", encoding="utf8") as f:
    version = re.search(r'__version__ = "(.*?)"', f.read()).group(1)

here = path.abspath(path.dirname(__file__))

with codecs.open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='nwpc-workflow-log-tool',

    version=version,

    description='Tools for workflow log in NWPC.',
    long_description=long_description,
    long_description_content_type='text/markdown',

    url='https://github.com/nwpc-oper/nwpc-workflow-log-tool',

    author='perillaroc',
    author_email='perillaroc@gmail.com',

    license='GPLv3',

    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7'
    ],

    keywords='nwpc workflow log model',

    packages=find_packages(exclude=['docs', 'tests', "legacy"]),

    install_requires=[
        "pyyaml",
        "loguru",
        "click",
        "nwpc-workflow-log-model>=3.0.0a2,<3.0.1",
        "nwpc-workflow-log-collector>=3.0.0a0,<3.0.1",
        "pandas",
        "numpy",
        "scipy",
        "tqdm",
        "bokeh",
    ],

    extras_require={
        'test': ['pytest'],
        'cov': ['pytest-cov', 'codecov']
    },

    entry_points={
        "console_scripts": [
            "workflow_log_analytic = nwpc_workflow_log_collector.analytic:cli",
        ],
    }
)
