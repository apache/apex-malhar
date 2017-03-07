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

import os 
dir_path = os.path.dirname(os.path.realpath(__file__))
print dir_path
input_data=dir_path+"/"+"/resources/stock_data.csv"
data = []
with open( input_data, "r") as outfile:
    outfile.readline()
    for line in outfile:
	data.append(line)

from pyapex import createApp

def filter_func(a):
  input_data=a.split(",")
  if float(input_data[2])> 45:
     return True
  return False


from pyapex import createApp
a=createApp('python_app').from_data(data) \
  .filter('filter_operator',filter_func) \
  .to_console(name='endConsole') \
  .launch(False)

