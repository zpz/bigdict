import pathlib
from setuptools import setup, find_packages


HERE = pathlib.Path(__file__).parent
README = (HERE / 'README.md').read_text()

name = 'bigdict'
version = next(open(HERE / f'src/{name}/__init__.py'))
version = version.strip('\n').split('=')[1].strip(" '")

setup(
    name=name,
    version=version,
    description='A persisted, out-of-memory dict for Python',
    long_description=README,
    long_description_content_type='text/markdown',
    url='https://github.com/zpz/' + name,
    author='zpz',
    author_email='zepu.zhang@gmail.com',
    license='MIT',
    python_requires='>=3.7',
    package_dir={'': 'src'},
    packages=[name],
    include_package_data=True,
    install_requires=[
        'rocksdb==0.7.0',
    ],
)
