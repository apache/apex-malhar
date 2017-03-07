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
package org.apache.apex.malhar.python.runtime;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.PythonConstants;
import org.apache.commons.lang3.StringUtils;

import static org.apache.apex.malhar.PythonConstants.PY4J_SRC_ZIP_FILE_NAME;
import static org.apache.apex.malhar.PythonConstants.PYTHON_APEX_ZIP_NAME;
import static org.apache.apex.malhar.PythonConstants.PYTHON_WORKER_FILE_NAME;

public class PythonWorkerContext implements Serializable
{
  public static String PY4J_DEPENDENCY_PATH = "PY4J_DEPENDENCY_PATH";
  public static String PYTHON_WORKER_PATH = "PYTHON_WORKER_PATH";
  public static String PYTHON_APEX_PATH = "PYTHON_APEX_PATH";
  public static String ENV_VAR_PYTHONPATH = "PYTHONPATH";

  private static final Logger LOG = LoggerFactory.getLogger(PythonWorkerContext.class);

  private String py4jDependencyPath = null;
  private String apexSourcePath = null;
  private String workerFilePath = null;
  private String pythonEnvPath = null;
  private byte[] serializedFunction = null;
  private PythonConstants.OpType opType = null;

  // environment data is set explicitly with local paths for managing local mode execution
  private Map<String, String> environmentData = new HashMap<String, String>();

  public PythonWorkerContext()
  {

  }

  public PythonWorkerContext(PythonConstants.OpType operationType, byte[] serializedFunction, Map<String, String> environmentData)
  {
    this();
    this.opType = operationType;
    this.serializedFunction = serializedFunction;
    this.environmentData = environmentData;
  }

  public void setup()
  {
    LOG.info("Setting up worker context: {}", this);
    LOG.debug("Final python environment path with Py4j depenency path: {}", pythonEnvPath);

    if ((this.apexSourcePath = environmentData.get(PYTHON_APEX_PATH)) == null) {
      this.apexSourcePath = new File("./" + PYTHON_APEX_ZIP_NAME).getAbsolutePath();
    }

    if ((this.py4jDependencyPath = environmentData.get(PY4J_DEPENDENCY_PATH)) == null) {
      this.py4jDependencyPath = new File("./" + PY4J_SRC_ZIP_FILE_NAME).getAbsolutePath();
    }

    LOG.info("FINAL WORKER PATH: {}", environmentData.get(PYTHON_WORKER_PATH));
    if ((this.workerFilePath = environmentData.get(PYTHON_WORKER_PATH)) == null) {
      File pythonWorkerFile = new File("./" + PYTHON_WORKER_FILE_NAME);
      this.workerFilePath = pythonWorkerFile.getAbsolutePath();
    }

    List<String> dependencyFilePaths = new ArrayList<String>();
    dependencyFilePaths.add(this.apexSourcePath);
    dependencyFilePaths.add(this.py4jDependencyPath);

    pythonEnvPath = System.getenv(ENV_VAR_PYTHONPATH);
    LOG.info("Found python environment path: {}", pythonEnvPath);
    if (pythonEnvPath != null) {
      dependencyFilePaths.add(pythonEnvPath);
      pythonEnvPath = StringUtils.join(dependencyFilePaths, ":");
    } else {
      pythonEnvPath = StringUtils.join(dependencyFilePaths, ":");
    }
    LOG.info("Python dependency Path {} worker Path {}", this.py4jDependencyPath, this.workerFilePath);
  }

  public synchronized String getPy4jDependencyPath()
  {
    return this.py4jDependencyPath;
  }

  public synchronized String getWorkerFilePath()
  {
    return this.workerFilePath;
  }

  public synchronized String getPythonEnvPath()
  {
    return this.pythonEnvPath;
  }

  public synchronized byte[] getSerializedFunction()
  {
    return this.serializedFunction;
  }

  public synchronized Map<String, String> getEnvironmentData()
  {
    return this.environmentData;
  }

  public synchronized void setEnvironmentData(Map<String, String> environmentData)
  {
    this.environmentData = environmentData;
  }

  public String getApexSourcePath()
  {
    return apexSourcePath;
  }

  public void setApexSourcePath(String apexSourcePath)
  {
    this.apexSourcePath = apexSourcePath;
  }

}
