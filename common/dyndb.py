"""Helpers to work with DynamoDB"""

import boto3
from .credentials import DynDBConnArgs

__author__ = "Alex Ganin"


class DynDBMgr:
  """Wrapper around boto3 resource and client interfaces"""

  def __init__(self):
    self._Res = boto3.resource('dynamodb', **DynDBConnArgs)


  def table(self, table_name):
    return self._Res.Table(table_name)
