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
package org.apache.apex.malhar.lib.appdata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.io.SimpleSinglePortInputOperator.BufferingOutputPort;

import com.google.common.base.Preconditions;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Sink;

/**
 * @since 3.3.0
 */
public class StoreUtils
{
  /**
   * This is a utility method which is used to attach the output port of an {@link EmbeddableQueryInfoProvider} to the input port
   * of the encapsulating {@link AppData.Store}.
   * @param <T> The type of data emitted by the {@link EmbeddableQueryInfoProvider}'s output port and received by the
   * {@link AppData.Store}'s input port.
   * @param outputPort The output port of the {@link EmbeddableQueryInfoProvider} which is being used by an {@link AppData.Store}.
   * @param inputPort The input port of the {@link AppData.Store} which is using an {@link EmbeddableQueryInfoProvider}.
   */
  public static <T> void attachOutputPortToInputPort(DefaultOutputPort<T> outputPort, final DefaultInputPort<T> inputPort)
  {
    outputPort.setSink(new Sink<Object>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public void put(Object tuple)
      {
        LOG.debug("processing tuple");
        inputPort.process((T)tuple);
      }

      @Override
      public int getCount(boolean reset)
      {
        return 0;
      }

    });
  }

  /**
   * This is a utility class which is responsible for flushing {@link BufferingOutputPort}s.
   * @param <TUPLE_TYPE> The type of the tuple emitted by the {@link BufferingOutputPort}.
   */
  public static class BufferingOutputPortFlusher<TUPLE_TYPE> implements Runnable
  {
    private final BufferingOutputPort<TUPLE_TYPE> port;

    public BufferingOutputPortFlusher(BufferingOutputPort<TUPLE_TYPE> port)
    {
      this.port = Preconditions.checkNotNull(port);
    }

    @Override
    public void run()
    {
      port.flush(Integer.MAX_VALUE);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(StoreUtils.class);
}
