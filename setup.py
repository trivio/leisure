import os.path
from setuptools import setup, find_packages

HERE = os.path.abspath(os.path.dirname(__file__))

README_PATH = os.path.join(HERE, 'README.md')
try:
    README = open(README_PATH).read()
except IOError:
    README = ''

with open('requirements.txt') as f:
  required = f.read().splitlines()

setup(
  name='leisure',
  #py_modules = ['leisure'],
  packages=find_packages(),
  version='0.0.4',
  description='local job runner for disco',
  long_description=README,
  author='Scott Robertson',
  author_email='scott@triv.io',
  url='http://github.com/trivio/leisure',
  classifiers=[
      "Programming Language :: Python",
      "License :: OSI Approved :: MIT License",
      "Operating System :: OS Independent",
      "Development Status :: 3 - Alpha",
      "Intended Audience :: Developers",
      "Topic :: Software Development",
  ],
  entry_points={
    'console_scripts': [
      'leisure = leisure:main'
    ]
  },
  install_requires=required
)
