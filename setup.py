from setuptools import find_packages, setup

setup(
    name='sickness_absence_publication',
    packages=find_packages(),
    version='0.1.0',
    description='To create publication ...',
    author='NHS_Digital',
    license='MIT',
    setup_requires=['pytest-runner','flake8'],
    tests_require=['pytest'],
)
