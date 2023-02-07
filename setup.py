from setuptools import find_packages, setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='jDocument',
    packages=find_packages(include=['jDocument']),
    version='0.1.11',
    description='The jDocument class allows you to encapsulate a json document (dict or a list) and perform a lot of operations to read, update and add data.',
    description_content_type='text/plain',
    long_description=long_description,
    long_description_content_type='text/x-rst',
    author='Jose Cordeiro',
    license='MIT',
    setup_requires=['Unidecode~=1.3.6', 'python-dateutil~=2.8.2'],
)
