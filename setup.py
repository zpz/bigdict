import pathlib
from setuptools import setup, find_packages


HERE = pathlib.Path(__file__).parent
README = (HERE / 'README.md').read_text()


setup(
    name='bigdict',
    version='21.4.11',
    description='A persisted, out-of-memory dict for Python',
    long_description=README,
    long_description_content_type='text/markdown',
    url='https://github.com/zpz/bigdict',
    author='Zepu Zhang',
    author_email='zepu.zhang@gmail.com',
    license='MIT',
    python_requires='>=3.7',
    package_dir={'': 'src'},
    packages=['bigdict'],
    include_package_data=True,
    install_requires=[
        'rocksdb==0.7.0',
    ],
)
