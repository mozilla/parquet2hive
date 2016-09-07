from setuptools import setup

setup(name='parquet2hive',
      version='0.2.11',
      author='Roberto Agostino Vitillo',
      author_email='rvitillo@mozilla.com',
      description='Hive import statement generator for Parquet datasets',
      url='https://github.com/mozilla/parquet2hive',
      data_files=[('share/parquet2hivemodules', ['parquet2hivemodules/parquet-tools.jar'])],
      scripts=['parquet2hive'],
      packages=['parquet2hivemodules'],
      install_requires=['boto3', 'functools32'],
      setup_requires=['pytest-runner'],
      tests_require=['pytest'])
