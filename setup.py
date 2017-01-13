from setuptools import setup

setup(name='parquet2hive',
      version='0.2.19',
      author='Roberto Agostino Vitillo',
      author_email='rvitillo@mozilla.com',
      description='Hive import statement generator for Parquet datasets',
      url='https://github.com/mozilla/parquet2hive',
      scripts=['parquet2hive'],
      packages=['parquet2hive_modules', 'parquet2hive_modules.parquet_format'],
      install_requires=['boto3', 'functools32', 'thrift==0.10.0', 'boto>=2.36.0'],
      setup_requires=['pytest-runner'],
      tests_require=['pytest', 'moto', 'wheel[signatures]', 'jsonschema'])
