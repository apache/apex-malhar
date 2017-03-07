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

from py4j.java_gateway import JavaGateway,java_import


class ShellConnector(object):
  gateway = None
  entry_point = None

  def __init__(self):
    self.gateway = JavaGateway()
    java_import(self.gateway.jvm, 'org.joda.time.*')
    java_import(self.gateway.jvm, 'org.apache.apex.malhar.lib.window.*')

  def __new__(cls):
    if not hasattr(cls, 'instance'):
      cls.instance = super(ShellConnector, cls).__new__(cls)
      return cls.instance

  def get_jvm_gateway(self):
    return self.gateway

  def get_entry_point(self):
    return self.gateway.entry_point
