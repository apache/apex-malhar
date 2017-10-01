#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

from abc import ABCMeta, abstractmethod

gateway = None

class AbstractPythonWorker(object):
  __metaclass__ = ABCMeta

  @abstractmethod
  def setFunction(self, f, opType):
    pass

  @abstractmethod
  def execute(self, tupleIn):
    pass

  class Java:
    implements = ["org.apache.apex.malhar.python.operator.interfaces.PythonWorker"]

class WorkerImpl(AbstractPythonWorker):
  serialized_f = None
  callable_f = None
  opType = None
  dataMap = None
  counter = 0;

  def __init__(self, gateway, opType):
    self.gateway = gateway
    self.opType = opType

  def setFunction(self, f, opType):
    try:
      import os, imp
      import dill
      self.callable_f = dill.loads(f)
      self.opType = opType
    except ValueError as e:
      print str(e)
      from traceback import print_exc
      print_exc()
    except Exception:
      from traceback import print_exc
      print_exc()
    return None

  def factory(gateway, type):
    if type == "MAP": return MapWorkerImpl(gateway, type)
    if type == "FLAT_MAP": return FlatMapWorkerImpl(gateway, type)
    if type == "FILTER": return FilterWorkerImpl(gateway, type)

  factory = staticmethod(factory)

class MapWorkerImpl(WorkerImpl):
  def execute(self, tupleIn):
    try:
      result = self.callable_f(tupleIn)
      return result
    except ValueError as e:
      print str(e)
      from traceback import print_exc
      print_exc()
    except Exception:
      from traceback import print_exc
      print_exc()
    return None

class FlatMapWorkerImpl(WorkerImpl):
  def execute(self, tupleIn):
    try:
      result = self.callable_f(tupleIn)
      from py4j.java_collections import SetConverter, MapConverter, ListConverter
      return ListConverter().convert(result, self.gateway._gateway_client)
      return result
    except ValueError as e:
      print str(e)
      from traceback import print_exc
      print_exc()
    except Exception:
      from traceback import print_exc
      print_exc()
    return None

class FilterWorkerImpl(WorkerImpl):
  def execute(self, tupleIn):
    try:
      result = self.callable_f(tupleIn)
      if type(result) != bool:
        result = True if result is not None else False
      return result
    except ValueError as e:
      print str(e)
      from traceback import print_exc
      print_exc()
    except Exception:
      from traceback import print_exc
      print_exc()
    return None

