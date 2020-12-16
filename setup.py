from setuptools import setup, find_packages

__version__ = '0.1.5'

requirements = [
    "boto3>=1.16.25",
    "pyhocon>=0.3.57",
    "amundsen-databuilder>=4.0.3",
    "ruamel.yaml>=0.16.12",
]

setup(
    name='flyover',
    version=__version__,
    description='Carte Flyover – extract metadata from data storage into files',
    url='https://github.com/CarteData/carte-flyover',
    maintainer='Balint Haller',
    maintainer_email='balint@haller.io',
    packages=find_packages(exclude=['tests*']),
    entry_points='''
        [console_scripts]
        flyover=flyover.__main__:main
    ''',
    dependency_links=[],
    install_requires=requirements,
    python_requires='>=3.6',
    classifiers=[
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
)
