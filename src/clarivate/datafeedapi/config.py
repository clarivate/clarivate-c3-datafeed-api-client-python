#!/usr/bin/env python

import os

# Connection and authentication details
SERVER_URL  =   'https://api.clarivate.com/ric/download/v1/'
API_KEY     =   os.environ.get("DATAFEED_API_KEY", None)

# Poll wait time in seconds
POLL_WAIT   =   60

# Directory for saving output
OUT_DIR     =   'datafeed_packages'

# Number of times to retry a download
RETRIES     =   5

# Time in seconds between retries
RETRY_WAIT  =   5

# Maximimum simultaneous connections
MAX_CONNECT =   10

