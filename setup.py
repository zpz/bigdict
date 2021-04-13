import pathlib
from setuptools import setup, find_packages


HERE = pathlib.Path(__file__).parent
README = (HERE / 'README.md').read_text()

version = next(open(HERE / 'src/bigdict/__init__.py'))
version = version.strip('\n').split('=')[1].strip(" '")

setup(
    name='bigdict',
    version=version,
    description='A persisted, out-of-memory dict for Python',
    long_description=README,
    long_description_content_type='text/markdown',
    url='https://github.com/zpz/bigdict',
    author='zpz',
    license='MIT',
    python_requires='>=3.7',
    package_dir={'': 'src'},
    packages=['bigdict'],
    include_package_data=True,
    install_requires=[
        'rocksdb==0.7.0',
    ],
)
