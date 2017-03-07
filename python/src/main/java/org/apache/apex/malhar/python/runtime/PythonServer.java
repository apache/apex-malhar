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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.PythonConstants;
import org.apache.apex.malhar.python.operator.proxy.PythonAcummlationWorkerProxy;
import org.apache.apex.malhar.python.operator.proxy.PythonWorkerProxy;
import org.apache.apex.malhar.python.util.LoggerUtils;
import org.apache.apex.malhar.python.util.NetworkUtils;

import py4j.GatewayServer;
import py4j.GatewayServerListener;
import py4j.Py4JServerConnection;

public class PythonServer
{

  private static final Logger LOG = LoggerFactory.getLogger(PythonServer.class);

  private byte[] serializedFunction = null;

  private PythonWorkerContext pythonWorkerContext = null;

  private PythonWorkerProxy proxy = null;
  protected Map<String, String> environementData = new HashMap<String, String>();
  protected transient GatewayServer gatewayServer = null;
  protected transient PythonGatewayServerListenser py4jListener = null;
  private PythonConstants.OpType operationType = null;

  public PythonServer()
  {

  }

  public PythonServer(PythonConstants.OpType operationType, byte[] serializedFunction)
  {

    this.serializedFunction = serializedFunction;
    this.operationType = operationType;
    this.pythonWorkerContext = new PythonWorkerContext(this.operationType, serializedFunction, environementData);

  }

  public boolean setup()
  {

    LOG.debug("LAUNCHING GATEWAY SERVER...{}", this.pythonWorkerContext);
    // Setting up context path explicitly for handling local as well Hadoop Based Application Development
    this.pythonWorkerContext.setup();

    if (proxy == null) {
      proxy = new PythonWorkerProxy<>(this.serializedFunction);
    }
    // Instantiating Py4j Gateway Server for Python Worker Process connect back
    boolean gatewayServerLaunchSuccess = false;
    int serverStartAttempts = 5;
    while (!gatewayServerLaunchSuccess && serverStartAttempts > 0) {
      try {
        this.gatewayServer = new GatewayServer(proxy, NetworkUtils.findAvaliablePort());
        this.py4jListener = new PythonGatewayServerListenser(this.gatewayServer, this.pythonWorkerContext);
        this.py4jListener.setOperationType(this.operationType);
        this.gatewayServer.addListener(this.py4jListener);
        this.gatewayServer.start(true);
        gatewayServerLaunchSuccess = true;
        --serverStartAttempts;
      } catch (Exception ex) {
        LOG.error("Gateway server failed to launch to due: {}" + ex.getMessage());
        gatewayServerLaunchSuccess = false;
      }
    }

    LOG.debug("LAUNCHING GATEWAY SERVER...");
    if (!gatewayServerLaunchSuccess) {
      throw new RuntimeException("Failed to launch Gateway Server");
    }

    serverStartAttempts = 5;

    while (!this.py4jListener.isPythonServerStarted() && !proxy.isFunctionEnabled() && serverStartAttempts > 0) {
      try {
        Thread.sleep(500L);
        LOG.debug("Waiting for Python Worker Registration");
        --serverStartAttempts;
      } catch (InterruptedException ex) {
        LOG.error("Python Callback server failed to launch to due: {}" + ex.getMessage());
      }
    }
    if (!proxy.isWorkerRegistered()) {
      this.gatewayServer.shutdown();
      throw new RuntimeException("Failed to launch Call Back Server");
    }

    // Transferring serialized function to Python Worker.
    LOG.debug("Checking if worker is registered {} {} ", proxy.isWorkerRegistered(), this.operationType);
    if (proxy.isWorkerRegistered() && !(proxy instanceof PythonAcummlationWorkerProxy)) {
      LOG.debug("Setting serialized function back ");
      proxy.setSerializedData(this.operationType.getType());
    }
    return true;
  }

  public void shutdown()
  {
    gatewayServer.shutdown();
  }

  public void setPythonOperatorEnv(Map<java.lang.String, java.lang.String> environementData)
  {
    this.environementData = environementData;
    if (pythonWorkerContext == null) {
      this.pythonWorkerContext = new PythonWorkerContext(this.operationType, serializedFunction, environementData);
    } else {
      this.pythonWorkerContext.setEnvironmentData(environementData);
    }
  }

  public static class PythonGatewayServerListenser implements GatewayServerListener
  {

    private GatewayServer server = null;
    private Process pyProcess = null;
    private boolean pythonServerStarted = false;

    private PythonConstants.OpType operationType = null;
    private static final Logger LOG = LoggerFactory.getLogger(PythonGatewayServerListenser.class);
    private PythonWorkerContext context = null;

    public boolean isPythonServerStarted()
    {
      return this.pythonServerStarted;
    }

    public PythonGatewayServerListenser(GatewayServer startedServer, PythonWorkerContext context)
    {
      this.server = startedServer;
      this.context = context;
    }

    public void connectionError(Exception e)
    {
      LOG.debug("Python Connection error : {}", e.getMessage());

    }

    @Override
    public void connectionStarted(Py4JServerConnection py4JServerConnection)
    {
      LOG.debug("Python Connection started: {}", py4JServerConnection.getSocket().getPort());

    }

    @Override
    public void connectionStopped(Py4JServerConnection py4JServerConnection)
    {
      LOG.debug("Python Connection stopped: {}", py4JServerConnection.getSocket().getPort());
      if (this.pyProcess != null) {
        this.pyProcess.destroy();
        LOG.debug("Destroyed python worker process");
      }
      LOG.debug("Restarting python worker process");
      this.startPythonWorker(this.server.getPort());
    }

    public void serverError(Exception e)
    {
      LOG.debug("Gateway Server error: {}", e.getMessage());
    }

    public void serverPostShutdown()
    {

      LOG.debug("Gateway server shut down");
    }

    public void serverPreShutdown()
    {
      LOG.debug("Gateway server shutting down");

      if (this.pyProcess != null) {
        this.pyProcess.destroy();
        LOG.debug("Destroyed python worker process");
      }
    }

    public void serverStarted()
    {
      LOG.debug("Gateway server started: {}", this.server.getPort());
      this.startPythonWorker(this.server.getPort());
    }

    public void serverStopped()
    {
      LOG.debug("Gateway server stopped");
      if (this.pyProcess != null) {
        this.pyProcess.destroy();
        LOG.debug("Destroyed python worker process");
      }

    }

    private void startPythonWorker(int gatewayServerPort)
    {
      ProcessBuilder pb = new ProcessBuilder(new java.lang.String[0]);
      try {
        LOG.info("Starting python worker process using context: {}", this.context);
        LOG.info("Worker File Path: {}", this.context.getWorkerFilePath());
        LOG.info("Python Environment Path: {}", this.context.getPythonEnvPath());
        Map<java.lang.String, java.lang.String> processEnvironment = pb.environment();
        processEnvironment.put("PYTHONPATH", this.context.getPythonEnvPath());
        this.pyProcess = pb.command(new java.lang.String[]{"/usr/bin/python", "-u", this.context.getWorkerFilePath(), "" + gatewayServerPort, operationType.getType()}).start();
        LoggerUtils.captureProcessStreams(this.pyProcess);
        this.pythonServerStarted = true;
        LOG.info("Python worker started: {} {} ", this.pyProcess, this.operationType);
      } catch (IOException exception) {

        LOG.error("Failed to start python server: {}" + exception.getMessage());
      }
    }

    public PythonConstants.OpType getOperationType()
    {
      return operationType;
    }

    public void setOperationType(PythonConstants.OpType operationType)
    {
      this.operationType = operationType;
    }

  }

  public PythonWorkerProxy getProxy()
  {
    return proxy;
  }

  public void setProxy(PythonWorkerProxy proxy)
  {
    this.proxy = proxy;
  }

  public PythonConstants.OpType getOperationType()
  {
    return operationType;
  }

  public void setOperationType(PythonConstants.OpType operationType)
  {
    this.operationType = operationType;
  }

}
