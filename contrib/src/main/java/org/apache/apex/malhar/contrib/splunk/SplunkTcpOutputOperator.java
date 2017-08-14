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
package org.apache.apex.malhar.contrib.splunk;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

import org.apache.apex.malhar.lib.db.AbstractStoreOutputOperator;

import com.splunk.TcpInput;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator;

/**
 * The output operator for Splunk, which writes to a TCP port on which splunk server is configured.
 * <p></p>
 * @displayName Splunk TCP Output
 * @category Output
 * @tags splunk
 * @since 1.0.4
 */
public class SplunkTcpOutputOperator<T> extends AbstractStoreOutputOperator<T, SplunkStore> implements Operator.CheckpointNotificationListener
{
  private String tcpPort;
  private transient Socket socket;
  private transient TcpInput tcpInput;
  private transient DataOutputStream stream;

  public String getTcpPort()
  {
    return tcpPort;
  }

  public void setTcpPort(String tcpPort)
  {
    this.tcpPort = tcpPort;
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    tcpInput = (TcpInput)store.getService().getInputs().get(tcpPort);
    try {
      socket = tcpInput.attach();
      stream = new DataOutputStream(socket.getOutputStream());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void processTuple(T tuple)
  {
    try {
      stream.writeBytes(tuple.toString());

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void beforeCheckpoint(long windowId)
  {
    try {
      stream.flush();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void checkpointed(long windowId)
  {
  }

  @Override
  public void committed(long windowId)
  {
  }

  @Override
  public void teardown()
  {
    super.teardown();
    try {
      stream.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      try {
        socket.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

}
