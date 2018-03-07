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

import java.util.Map;

import org.apache.apex.malhar.python.base.jep.JepPythonEngine;

/***
 * Used as key configs while passing the pre interpreter configuration. See
 *  {@link JepPythonEngine#preInitInterpreter(Map)}
 */
public enum PythonInterpreterConfig
{
  PYTHON_INCLUDE_PATHS,
  PYTHON_SHARED_LIBS,
  IDLE_INTERPRETER_SPIN_POLICY,
  REQUEST_QUEUE_WAIT_SPIN_POLICY,
  SLEEP_TIME_MS_IN_CASE_OF_NO_REQUESTS;

}
