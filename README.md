# parquet2hive [![Build Status](https://travis-ci.org/mozilla/parquet2hive.svg?branch=master)](https://travis-ci.org/mozilla/parquet2hive)
Hive import statement generator for Parquet datasets. Supports versioned datasets and schema evolution.

## Installing from Pypi 
To install this package from Pypi, run:

```bash
pip install parquet2hive
```

## Updating the Package on PyPi
To upload the most recent version, run:

```bash
python setup.py sdist upload
```

## Using the TestPypi Servers
You will need a separate account on https://testpypi.python.org.
To upload the file to the pypi test servers, ensure your ```~/.pypirc``` contains the following:

```bash
[distutils]
index-servers=
    pypi
    pypitest

[pypitest]
repository = https://testpypi.python.org/pypi
username = testpypi_username 
password = testpypi_password 

[pypi]
repository = https://pypi.python.org/pypi
username = pypi_username 
password = pypi_password   
```

Upload the code using:
```bash
python setup.py sdist upload -r https://testpypi.python.org/pypi
```

Finally, pull the most recent package from the test-repository on any machine using:
```bash
pip install parquet2hive -i https://testpypi.python.org/pypi
```

## Example usage
```bash
parquet2hive s3://telemetry-parquet/longitudinal | bash
```

To see the allowed command line interface arguments, run ```parquet2hive -h```
