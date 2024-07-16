import setuptools
import os

setuptools.setup(
    name='find-a-bug',
    version='0.1',     
    url='https://github.com/pipparichter/find-a-bug',
    author='Philippa Richter',
    author_email='prichter@caltech.edu',
    packages=['setup', 'update', 'utils'],
    install_requires=setuptools.find_packages(exclude=['setup', 'update', 'utils']))
