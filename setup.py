from setuptools import find_packages, setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='jDocument',
    packages=find_packages(include=['jDocument']),
    version='0.1.0',
    long_description=long_description,
    long_description_content_type='text/plain',
    author='Jose Carlos Cordeiro Martins',
    license='MIT',
    setup_requires=['Unidecode~=1.3.6', 'python-dateutil~=2.8.2'],
)
