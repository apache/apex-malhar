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
import org.apache.apex.malhar.lib.window.impl.KeyedWindowedOperatorImpl;
import org.apache.apex.malhar.python.operator.proxy.PythonWorkerProxy;
import org.apache.apex.malhar.python.runtime.PythonServer;

import com.datatorrent.api.Context;

public class PythonKeyedWindowedOperator extends KeyedWindowedOperatorImpl
{

  private static final Logger LOG = LoggerFactory.getLogger(PythonWindowedOperator.class);
  private transient PythonServer server = null;
  protected byte[] serializedFunction = null;
  protected transient PythonConstants.OpType operationType = null;

  public PythonKeyedWindowedOperator()
  {
    this.serializedFunction = null;
  }

  public PythonKeyedWindowedOperator(byte[] serializedFunc)
  {
    this.serializedFunction = serializedFunc;
    this.server = new PythonServer(this.operationType, serializedFunc);
  }

  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    server.setOperationType(((PythonWorkerProxy)this.accumulation).getOperationType());
    server.setProxy((PythonWorkerProxy)this.accumulation);
    server.setup();
  }

  public void teardown()
  {
    if (server != null) {
      server.shutdown();
    }
  }
}
