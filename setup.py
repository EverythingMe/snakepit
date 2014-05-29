from setuptools import setup, find_packages
from pip.req import parse_requirements
from os import path

requirements = path.join(path.abspath(path.dirname(__file__)), 'requirements.txt')

setup(
    name='snakepit',
    author='EverythingMe',
    version='0.1',
    packages=find_packages(exclude=['*.test', '*example*']),
    install_requires=[str(r.req) for r in parse_requirements(requirements)]
)
