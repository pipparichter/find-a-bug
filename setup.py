import setuptools
import os

print(setuptools.find_packages())

setuptools.setup(
    name='Find-A-Bug',
    version='0.1',     
    url='https://github.com/pipparichter/find-a-bug',
    author='Philippa Richter',
    author_email='prichter@caltech.edu',
    packages=['setup', 'update', 'utils', 'app'],
    install_requires=setuptools.find_packages(exclude=['setup', 'update', 'utils', 'app']))
