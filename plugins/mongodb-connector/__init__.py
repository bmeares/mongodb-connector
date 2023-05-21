#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Provide `MongoDBConnector`.
"""

required = ['pymongo', 'python-dateutil']

from meerschaum.connectors import make_connector
from .mongodb_connector import MongoDBConnector
