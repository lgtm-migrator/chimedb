from setuptools import setup, find_packages

import versioneer

setup(
    name="chimedb",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    packages=find_packages(),
    zip_safe=False,
    install_requires=[
        "mysql-connector-python >= 8.0, <= 8.0.29",
        "peewee >= 3.12",
        "sshtunnel >= 0.4.0",
        "ujson",
        "future",
        "PyYAML",
    ],
    author="The CHIME Collaboration",
    author_email="dvw@phas.ubc.ca",
    description="Low-level CHIME database access",
    license="GPL v3.0",
    url="https://github.com/chime-experiment/chimedb",
    extras_require={
        "tests": ["pytest"],
    },
)
