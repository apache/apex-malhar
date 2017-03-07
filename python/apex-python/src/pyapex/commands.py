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

from pyapex import getApp
import getopt
import sys

def kill_app(app_name):
  app = getApp(app_name)
  if app:
    print "KILLING APP " + str(app_name)
    app.kill()
  else:
    print "Application not found for name " + str(app_name)


def shutdown_app(app_name):
  app = getApp(app_name)
  if app:
    print "SHUTTING DOWN APP NOW"
    app.kill()
  else:
    print "Application not found for name " + str(app_name)


func_map = {"KILL_MODE": kill_app, "SHUTDOWN_MODE": shutdown_app}


def parse_argument():
  print sys.argv
  try:
    opts, args = getopt.getopt(sys.argv[1:], "k:s:")
  except getopt.GetoptError as err:
    # print help information and exit:
    print str(err)  # will print something like "option -a not recognized"
    sys.exit(2)
  for opt, args in opts:
    if opt == '-k':
      func_map['KILL_MODE'](args)
    elif opt == '-s':
      func_map['KILL_MODE'](args)
    else:
      assert False, "Invalid Option"


if __name__ == "__main__":
  parse_argument()
