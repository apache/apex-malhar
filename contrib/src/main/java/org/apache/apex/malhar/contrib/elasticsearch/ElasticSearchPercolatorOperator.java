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
package org.apache.apex.malhar.contrib.elasticsearch;

import java.io.IOException;

import javax.validation.constraints.NotNull;

import org.elasticsearch.action.percolate.PercolateResponse;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * Percolate operator for ElasticSearch
 *
 * @category Output
 * @tags elastic search
 * @since 2.1.0
 */
public class ElasticSearchPercolatorOperator extends BaseOperator
{
  @NotNull
  public String hostName;
  public int port;

  @NotNull
  public String indexName;
  @NotNull
  public String documentType;

  protected transient ElasticSearchPercolatorStore store;
  public final transient DefaultOutputPort<PercolateResponse> outputPort = new DefaultOutputPort<PercolateResponse>();

  public final transient DefaultInputPort<Object> inputPort = new DefaultInputPort<Object>()
  {
    /*
     * (non-Javadoc)
     *
     * @see com.datatorrent.api.DefaultInputPort#process(java.lang.Object)
     */
    @Override
    public void process(Object tuple)
    {

      PercolateResponse response = store.percolate(new String[] {indexName}, documentType, tuple);
      outputPort.emit(response);
    }
  };

  public void setup(com.datatorrent.api.Context.OperatorContext context)
  {
    store = new ElasticSearchPercolatorStore(hostName, port);
    try {
      store.connect();
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
  }

  /* (non-Javadoc)
   * @see com.datatorrent.common.util.BaseOperator#teardown()
   */
  @Override
  public void teardown()
  {
    super.teardown();
    try {
      store.disconnect();
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
  }
}
