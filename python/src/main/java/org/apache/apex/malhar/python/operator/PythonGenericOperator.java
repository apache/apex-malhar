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
package org.apache.apex.malhar.python.operator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.PythonConstants;
import org.apache.apex.malhar.python.runtime.PythonServer;

import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

public abstract class PythonGenericOperator<T> extends BaseOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(PythonGenericOperator.class);
  protected byte[] serializedFunction = null;
  private PythonServer server = null;
  protected transient PythonConstants.OpType operationType = null;

  public final transient DefaultInputPort<T> in = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      processTuple(tuple);
    }

  };
  public final transient DefaultOutputPort<T> out = new DefaultOutputPort<T>();

  public PythonGenericOperator()
  {
    this(null, null);

  }

  public PythonGenericOperator(PythonConstants.OpType operationType, byte[] serializedFunc)
  {
    this.serializedFunction = serializedFunc;
    this.operationType = operationType;
    this.server = new PythonServer(this.operationType, serializedFunc);

  }

  public void setup(OperatorContext context)
  {
    LOG.debug("Application path from Python Operator: {} ", (String)context.getValue(DAGContext.APPLICATION_PATH));
    // Setting up context path explicitly for handling local as well Hadoop Based Application Development
    server.setup();

  }

  public void teardown()
  {
    if (server != null) {
      server.shutdown();
    }
  }

  public PythonServer getServer()
  {
    return server;
  }

  public void setServer(PythonServer server)
  {
    this.server = server;
  }

  protected abstract void processTuple(T tuple);

}
