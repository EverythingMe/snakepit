try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup
from pip.req import parse_requirements
from os import path

requirements = path.join(path.abspath(path.dirname(__file__)), 'requirements.txt')

setup(
    name='snakepit',
    author='EverythingMe',
    version='0.1',
    packages=['snakepit',
              'snakepit.zmq', 'snakepit.zmq.tornado',
              'snakepit.tornado', 'snakepit.zookeeper'],
    install_requires=[str(r.req) for r in parse_requirements(requirements)]
)