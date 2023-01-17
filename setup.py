from setuptools import find_packages, setup
setup(
    name='jDocument',
    packages=find_packages(include=['jDocument']),
    version='0.1.0',
    description='The jDocument class allows you to encapsulate a json document (dict or a list) and perform a lot of operations to read, update and add data',
    author='Jose Carlos Cordeiro Martins',
    license='MIT',
    setup_requires=['Unidecode~=1.3.6', 'python-dateutil~=2.8.2'],
)
