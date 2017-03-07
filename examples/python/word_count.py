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
input_data=dir_path+"/"+"/resources/hadoop_word_count.txt"
data = []
with open( input_data, "r") as outfile:
    outfile.readline()
    for line in outfile:
        for d in line.split(' '):
          if len(d):
	     data.append(d)


from pyapex import createApp
from pyapex.functions.window import TriggerType,Trigger,TriggerOption

t=TriggerOption.at_watermark()
t.firingOnlyUpdatedPanes()
t.accumulatingFiredPanes()
t.withEarlyFiringsAtEvery(count=4)


from pyapex import createApp
a=createApp('reduce_app2').from_data(data) \
  .window(window='TIME', duration=110, trigger=t,allowed_lateness=100) \
  .countByKey("countByKey") \
  .to_console(name='endConsole') \
  .launch(False)
