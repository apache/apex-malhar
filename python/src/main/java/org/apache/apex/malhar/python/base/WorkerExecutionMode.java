/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.python.base;

import org.apache.apex.malhar.python.base.requestresponse.PythonInterpreterRequest;

/***
 * <p>Used to specify if a given API invocation in the in-memory interpreter is going to be invoked for all the worker
 *  threads, a sticky thread or just any one thread.
 *
 *  - {@link WorkerExecutionMode#BROADCAST} is to be used when the
 *   command is resulting in a state of the interpreter which has to be used in subsequent calls. For example,
 *   deserializing a machine learning model can be used as a BROADCAST model as the scoring can then be invoked across
 *   all worker threads if required.
 *  - {@link WorkerExecutionMode#ANY} Represents a state wherein any worker can be used to execute the code.
 *   Example would be scoring an incoming tuple on which the model has already been deserialized across all nodes
 *  - {@link WorkerExecutionMode#STICKY} Use STICKY if the same worker needs to service the request.
 *    The downside of this is that it may or may not complete on time and depends on the queue length.</p>
 *
 *   <p><b>Ensure the {@link PythonInterpreterRequest#hashCode()} is overridden if STICKY is chosen </b></p>
 */
public enum WorkerExecutionMode
{
  BROADCAST,
  ANY,
  STICKY
}
