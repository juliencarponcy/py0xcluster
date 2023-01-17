#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
]

test_requirements = [ ]

setup(
    author="Julien Carponcy",
    author_email='juliencarponcy@gmail.com',
    python_requires='>=3.7',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="0xCluster is a package dedicated to build group of Ethereum addresses based on DEX activity",
    install_requires=requirements,
    license="GNU General Public License v3",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='py0xcluster',
    name='py0xcluster',
    packages=find_packages(include=['py0xcluster', 'py0xcluster.*']),
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/juliencarponcy/py0xcluster',
    version='0.0.2',
    zip_safe=False,
)
