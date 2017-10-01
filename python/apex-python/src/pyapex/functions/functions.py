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

class AbstractAccumulatorPythonWorker(object):
  __metaclass__ = ABCMeta


  @abstractmethod
  def setObject(self, obj, opType):
    pass

  @abstractmethod
  def defaultAccumulatedValue(self):
    pass


  @abstractmethod
  def getOutput(accumulated):
    pass


  @abstractmethod
  def getRetraction(output):
    pass

  @abstractmethod
  def accumulate(self, accumulated, input):
    pass

  @abstractmethod
  def merge(self, input1, input2):
    pass


class AccumulatorWorkerImpl(AbstractAccumulatorPythonWorker):

  accum_obj = None
  opType = None
  counter = 0;

  def __init__(self, gateway, opType):
    self.gateway = gateway
    self.opType = opType

  @abstractmethod
  def setObject(self, obj, opType):
    try:
      import os, imp
      import cloudpickle
      self.accum_obj = cloudpickle.loads(obj)
      self.opType = opType
    except ValueError as e:
      print str(e)
      from traceback import print_exc
      print_exc()
    except Exception:
      from traceback import print_exc
      print_exc()
    return "RETURN VALUE"


  def getConfirmed(self):
    return self.opType


class ReduceFunction(AbstractAccumulatorPythonWorker):

  def accumulate( self, accumulated, data ):
    return self.reduce(accumulated, data)


  def merge( self, input1, input2 ):
    return self.reduce(input1, input2)

  @abstractmethod
  def reduce( input1, input2 ):
    pass

  def defaultAccumulatedValue( self, data ):
    return data

  def getOutput( accumulated ):
    return accumulated

  def getRetraction( output ):
    return None

  def setObject( self, obj, opType ):
    pass

  class Java:
    implements = ["org.apache.apex.malhar.python.operator.interfaces.PythonReduceWorker"]