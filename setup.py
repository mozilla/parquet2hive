from setuptools import setup

setup(name='parquet2hive',
      version='0.2.3',
      author='Roberto Agostino Vitillo',
      author_email='rvitillo@mozilla.com',
      description='Hive import statement generator for Parquet datasets',
      url='https://github.com/vitillo/parquet2hive',
      data_files=[('share/parquet2hive', ['parquet2hive/parquet-tools.jar'])],
      scripts=['parquet2hive/parquet2hive'],
      install_requires=['boto3'])
