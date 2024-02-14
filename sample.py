#!/usr/bin/env python

"""
A simple example of how to use the clarivate datafeed API
"""

from clarivate import datafeedapi

datafeedapi.Client(api_key="YOUR API KEY HERE").fetch("understandDLFReports", "standard")
