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

import types

import dill
from uuid import uuid1
from py4j.protocol import Py4JJavaError
from shellconn import ShellConnector

def createApp(name):
  shellConnector = ShellConnector()
  return ApexStreamingApp(name, shellConnector)


def getApp(name):
  shellConnector = ShellConnector()
  java_app = shellConnector.get_entry_point().getAppByName(name)
  return ApexStreamingApp(name, java_app=java_app)

'''
This is Python Wrapper Around ApexStreamingApp
If java streaming app is not found then no apis can be called on this wrapper.
'''
class ApexStreamingApp():
  app_id = None
  streaming_factory = None
  java_streaming_app = None
  instance_id = None
  shell_connector = None
  serialized_file_list = []

  def __init__(self, name, shell_connector=None, java_app=None):
    if shell_connector is None and java_app is None:
      raise Exception("Invalid App initialization")
    if java_app is None:
      self.shell_connector = shell_connector
      self.java_streaming_app = shell_connector.get_entry_point().createApp(name)
    else:
      self.java_streaming_app = java_app
      self.shell_connector = shell_connector
      self.instance_id = uuid1().urn[9:]

  '''
      This fuction will initialize input adapter to read from hdfs adapter  
  '''
  def from_directory(self, folder_path):
    self.java_streaming_app = self.java_streaming_app.fromFolder(folder_path)
    return self

  def from_kafka08(self, zoopkeepers, topic):
    self.java_streaming_app = self.java_streaming_app.fromKafka08(zoopkeepers, topic)
    return self

  def from_kafka09(self, zoopkeepers, topic):
      self.java_streaming_app = self.java_streaming_app.fromKafka09(zoopkeepers, topic)
      return self

  '''
  Allow Data To be provided as input on shell or directly in python file such as
  
  '''
  def from_data(self, data):
    if not isinstance(data, list):
      raise Exception
    data_for_java = self.shell_connector.get_jvm_gateway().jvm.java.util.ArrayList()
    types_data = [int, float, str, bool, tuple, dict]
    for d in data:
      if type(d) in types_data:
        data_for_java.append(d)
    self.java_streaming_app = self.java_streaming_app.fromData(data_for_java)
    return self

  def to_console(self, name=None):
    self.java_streaming_app = self.java_streaming_app.toConsole(name)
    return self

  def to_kafka_08(self, name=None, topic=None, brokerList=None, **kwargs):
    properties = {}
    if brokerList is not None:
      properties['bootstrap.servers'] = brokerList
    for key in kwargs.keys():
      properties[key] = kwargs[key]
    property_map = self.shell_connector.get_jvm_gateway().jvm.java.util.HashMap()
    for key in properties.keys():
      property_map.put(key, properties[key])
    self.java_streaming_app = self.java_streaming_app.toKafka08(name, topic, property_map)
    return self

  def to_directory(self, name,file_name, directory_name, **kwargs):
    properties = {}
    if file_name is None or directory_name is None:
      raise Exception("Directory Name or File name should be specified")
    self.java_streaming_app = self.java_streaming_app.toFolder(name, file_name, directory_name)
    return self

  def map(self, name, func):
    if not isinstance(func, types.FunctionType):
      raise Exception

    serialized_func = self.serialize_function(name, func)
    self.java_streaming_app = self.java_streaming_app.map(name, serialized_func)
    return self

  def flatmap(self, name, func):
    if not isinstance(func, types.FunctionType):
      raise Exception
    serialized_func = self.serialize_function(name, func)
    self.java_streaming_app = self.java_streaming_app.flatMap(name, serialized_func)
    return self

  def filter(self, name, func):
    if not isinstance(func, types.FunctionType):
      raise Exception
    serialized_func = self.serialize_function(name, func)
    self.java_streaming_app = self.java_streaming_app.filter(name, serialized_func)
    return self

  def window(self,*args,**kwargs):
    _jwindow = None
    _jtrigger = None
    gateway=self.shell_connector.get_jvm_gateway()
    if 'window' not in kwargs or kwargs['window'] == 'GLOBAL':
      _jwindow=gateway.jvm.WindowOption.GlobalWindow()
    elif kwargs['window'] == 'TIME' and 'duration' in kwargs :
      duration = long(kwargs['duration'])
      _jduration = gateway.jvm.Duration(duration)
      _jwindow = gateway.jvm.WindowOption.TimeWindows(_jduration)

    elif kwargs['window'] == 'SLIDING' and 'duration' in kwargs and 'sliding_time' in kwargs:
      duration = long(kwargs['duration'])
      sliding_time = long(kwargs['sliding_time'])
      _jduration = gateway.jvm.Duration(duration)
      _jslideby = gateway.jvm.Duration(sliding_time)
      _jwindow = gateway.jvm.SlidingTimeWindows(_jduration,_jslideby)

    elif kwargs['window'] == 'SESSION' and 'mingap' in kwargs:
      mingap = long(kwargs['mingap'])
      _jmingap = gateway.jvm.Duration(mingap)
      _jwindow = gateway.jvm.SessionWindows(_jmingap)

    _jtrigger= None
    if 'trigger' in kwargs:
      from pyapex.functions.window import TriggerOption
      if isinstance(kwargs['trigger'], TriggerOption ):
          _jtrigger = TriggerOption.get_java_trigger_options(kwargs['trigger'],gateway)
      else:
          raise Exception("Incorrect Trigger Option")
      from pyapex.functions.window import TriggerOption
    _jallowed_lateness= None
    if 'allowed_lateness' in kwargs:
      _jallowed_lateness = gateway.jvm.Duration( long(kwargs['allowed_lateness']))
    self.java_streaming_app = self.java_streaming_app.window(_jwindow,_jtrigger,_jallowed_lateness)
    return self

  def count(self,count="counter"):
    self.java_streaming_app = self.java_streaming_app.count(name)
    return self

  def countByKey(self,name="counter"):
    self.java_streaming_app = self.java_streaming_app.countByKey(name)
    return self

  def reduce(self,name, reduceFn):
    serialized_func = self.serialize_function(name, reduceFn)
    self.java_streaming_app = self.java_streaming_app.reduce(name,serialized_func)
    return self

  def reduceByKey(self,name, reduceFn):
    serialized_func = self.serialize_function(name, reduceFn)
    self.java_streaming_app = self.java_streaming_app.reduceByKey(name,serialized_func)
    return self

  def launch(self, local_mode=False):
    try:
      self.app_id = self.java_streaming_app.launch(local_mode)
      return self.app_id
    except Py4JJavaError as e:
      import traceback
      traceback.print_exc()
      return e.java_exception.getMessage()

  def kill(self):
    return self.java_streaming_app.kill()

  def set_config(self, key, value):
    self.java_streaming_app = self.java_streaming_app.setConfig(key, value)
    return self

  def serialize_function(self, name, func):
    serialized_func = bytearray()
    serialized_func.extend(dill.dumps(func))
    return serialized_func
