from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='nwpc-work-flow-model',

    version='0.1.0',

    description='A work flow model using in operation systems at NWPC.',
    long_description=long_description,

    url='https://https://github.com/perillaroc/nwpc-work-flow-model',

    author='perillaroc',
    author_email='perillaroc@gmail.com',

    license='MIT',

    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6'
    ],

    keywords='nwpc workflow model',

    packages=find_packages(exclude=['contrib', 'docs', 'tests']),

    install_requires=[],

    extra_require={
        'test': ['pytest'],
    },

    entry_points={}
)
