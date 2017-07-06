/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.python.operator.runtime;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.PythonConstants;
import org.apache.apex.malhar.python.runtime.PythonWorkerContext;

//import org.apache.apex.malhar.python.operator.PythonGenericOperator;

public class PythonWorkerContextTest
{

  @Test
  public void testPythonWorkerContextTest()
  {
    PythonWorkerContext context = new PythonWorkerContext();
    String currentWorkingDirectory = "/home/data";
    Map<String, String> environmentData = new HashMap<>();
    environmentData.put(PythonWorkerContext.PY4J_DEPENDENCY_PATH, currentWorkingDirectory + "/./" + PythonConstants.PY4J_SRC_ZIP_FILE_NAME);
    environmentData.put(PythonWorkerContext.PYTHON_WORKER_PATH, currentWorkingDirectory + "/./" + PythonConstants.PYTHON_WORKER_FILE_NAME);

    context.setEnvironmentData(environmentData);
    context.setup();
    Assert.assertEquals(currentWorkingDirectory + "/./" + PythonConstants.PY4J_SRC_ZIP_FILE_NAME, context.getPy4jDependencyPath());
    Assert.assertEquals(currentWorkingDirectory + "/./" + PythonConstants.PYTHON_WORKER_FILE_NAME, context.getWorkerFilePath());

  }

  @Test
  public void testPythonWorkerContextWithDeafaultTest()
  {

    PythonWorkerContext context = new PythonWorkerContext();
    String currentWorkingDirectory = System.getProperty("user.dir");
    context.setup();
    Assert.assertEquals(currentWorkingDirectory + "/./" + PythonConstants.PY4J_SRC_ZIP_FILE_NAME, context.getPy4jDependencyPath());
    Assert.assertEquals(currentWorkingDirectory + "/./" + PythonConstants.PYTHON_WORKER_FILE_NAME, context.getWorkerFilePath());


  }

}
