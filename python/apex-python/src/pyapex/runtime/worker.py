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

'''
Worker.py file is responsible for instantiating specific workers such as MapWorkerImpl, FlatMapWorkerImpl, FilterWorkerImpl.

Worker.py is ran using python and then we register back WorkerImpl with Java Process for each calls. 
'''
import sys
import site
from py4j.java_gateway import JavaGateway, CallbackServerParameters, GatewayParameters, java_import

# TODO this may cause race condition
def find_free_port():
  import socket
  s = socket.socket()
  s.listen(0)
  addr, found_port = s.getsockname()  # Return the port number assigned.
  s.shutdown(socket.SHUT_RDWR)
  s.close()
  return found_port


def main(argv):
  import os, getpass

  PYTHON_PATH = os.environ['PYTHONPATH'] if 'PYTHONPATH' in os.environ else None
  os.environ['PYTHONPATH'] = PYTHON_PATH + ':' + site.getusersitepackages().replace('/home/.local/',
                                                                                    '/home/' + getpass.getuser() + '/.local/') + '/'
  sys.path.extend(os.environ['PYTHONPATH'].split(':'))
  print "PYTHONPATH " + str(os.environ['PYTHONPATH'])
  gateway_params = GatewayParameters(address='127.0.0.1', port=int(argv[0]), auto_convert=True)
  callback_params = CallbackServerParameters(daemonize=False, eager_load=True, port=0)
  gateway = JavaGateway(gateway_parameters=gateway_params, callback_server_parameters=callback_params)

  # Retrieve the port on which the python callback server was bound to.
  python_port = gateway.get_callback_server().get_listening_port()

  # Register python callback server with java gateway server
  # Note that we use the java_gateway_server attribute that
  # retrieves the GatewayServer instance.
  gateway.java_gateway_server.resetCallbackClient(
    gateway.java_gateway_server.getCallbackClient().getAddress(),
    python_port)

  # Instantiate WorkerImpl for PythonWorker java interface and regsiter with PythonWorkerProxy in Java.
  from pyapex.functions import WorkerImpl
  print "Registering Python Worker "
  workerImpl = WorkerImpl.factory(gateway, argv[1])
  if argv[1] in ['REDUCE','REDUCE_BY_KEY']:
    serialized_object =  gateway.entry_point.getSerializedData()
    import dill
    workerImpl=dill.loads(serialized_object)
    print type(workerImpl)
    gateway.entry_point.register(workerImpl)
  else:
    gateway.entry_point.register(workerImpl)

  print "Python process started with type: " + argv[1]


if __name__ == "__main__":
  main(sys.argv[1:])
