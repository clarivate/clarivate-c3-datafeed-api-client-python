# Clarivate's DATA FEED-API

A client interface and sample code for Clarivate's Data Feed-API and the available content sets.

## Description

Clarivate Data Feed API provides consolidated access to bulk-download various Clarivate content sets from multiple domains.

## Getting Started

### Authorization

A valid API KEY is required.
first, you need a valid Clarivate subscription ([Sales enquiries](https://clarivate.com/contact-us/sales-enquiries/)).
then, you need to self-register for an API KEY into our [Clarivate Developer Portal](https://developer.clarivate.com/).

### Dependencies

- Poetry for Python package and environment management

The library is written in python and support python version >= 3.9.0, You can also use [Pyenv](https://github.com/pyenv/pyenv) for this

```
pyenv install 3.9.0
```

To switch current python version then you can execute

```bash
pyenv local 3.9.0
```

- Create a new virtual environment

```bash
python3 -m venv .venv
```

and activate it

```bash
 source <venv>/bin/activate  # POSIX
 <venv>\Scripts\activate.bat  # Windows CMD
 <venv>\Scripts\Activate.ps1  # Windows Powershell
```

Alternatively, you can use Intellij IDEA SDK Management

- Install poetry within virtual environment

```bash
python -m pip install poetry
```

- Install project dependencies

```bash
 poetry install
```

## How to use the library?

### Import

```
from clarivate import datafeedapi
```

### Examples

- Download a dataset in async mode. (example with all parameters)

```python
client = datafeedapi.Client('api key','server url')
response = client.fetch('dataset', 'preset', 'internal change number', 'output format', 'split files', 'extract resources')
```

- Download a dataset in sync mode. (example with all parameters)

```python
client = datafeedapi.Client('api key','server url')
response = client.fetch_sync('dataset', 'preset', 'internal change number', 'output format', 'split files', 'extract resources')
```

Note 1: 'api key' and 'server url' can be set into the config.py file.

Note 2: 'dataset', 'preset', 'internal change number', 'output format', 'split files' and 'extract resources' parameters have default settings.

## Help

### Support

- For technical help or questions regarding the repo, you can leave a message into github;
- You can also contact [John Ono](john.ono@clarivate.com) or [Pascal Vandenabeele](pascal.vandenabeele@clarivate.com)
- The Swagger documentation (API endpoints) is available from the dedicated page at [Clarivate Developer Portal](https://developer.clarivate.com/apis/ric-download-api).

### Authors

- © 2024 Clarivate

### License

This project is licensed under the MIT License - see the LICENSE.md file for details.

## Version History

### 1.0 - 2024/01/01

    * Initial Release
