from setuptools import setup
from setuptools import find_packages

with open('requirements.txt') as reqs:
    install_requires = [
        line for line in reqs.read().split('\n')
        if line and not line.startswith('--') and not line.startswith('-') and
        (";" not in line)]

setup(
    name='jsonsyslog2elasticsearch',
    url="https://github.com/metwork-framework/jsonsyslog2elasticsearch",
    packages=find_packages(),
    install_requires=install_requires,
    entry_points={
        "console_scripts": [
            "jsonsyslog2elasticsearch = jsonsyslog2elasticsearch:main"
        ]
    }
)
