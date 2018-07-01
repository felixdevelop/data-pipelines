import os
from setuptools import setup


def read(fname):
    path = os.path.join(os.path.dirname(__file__), fname)
    try:
        file = open(path, encoding='utf-8')
    except TypeError:
        file = open(path)
    return file.read()


setup(
    name='data-pipelines',
    version=__import__('data_pipelines').VERSION,
    description='Data Pipelines Framework',
    long_description=read('README.md'),
    long_description_content_type="text/markdown",
    author='Vadim Sharay',
    author_email='vadimsharay@gmail.com',
    url='https://github.com/felixfire/dpf',
    packages=["data_pipelines", "data_pipelines.blocks"],
    license='AGPLv3',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: Free for non-commercial use',
        'License :: OSI Approved :: GNU Affero General Public License v3',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Environment :: Web Environment',
        'Topic :: Software Development',
    ],
    zip_safe=False,
    include_package_data=True,
)
