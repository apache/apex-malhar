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
package org.apache.apex.malhar.lib.io;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.Client;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * This is the base implementation for HTTP operators.&nbsp;
 * This operator handles the creation and destruction of client connections.&nbsp;
 * Subclasses must implement the method which processes incoming tuples.
 * <p></p>
 * @displayName Abstract HTTP
 * @category Input
 * @tags http, input operator
 *
 * @param <T>
 * @since 1.0.2
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public abstract class AbstractHttpOperator<T> extends BaseOperator
{
  @NotNull
  protected String url;
  protected transient Client wsClient;

  /**
   * The input port which receives tuples for processing.
   */
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T t)
    {
      processTuple(t);
    }

  };

  protected abstract void processTuple(T t);

  @Override
  public void setup(OperatorContext context)
  {
    wsClient = Client.create();
    wsClient.setFollowRedirects(true);
    logger.debug("URL: {}", url);
  }

  @Override
  public void teardown()
  {
    if (wsClient != null) {
      wsClient.destroy();
    }
    super.teardown();
  }

  public String getUrl()
  {
    return url;
  }

  public void setUrl(String url)
  {
    this.url = url;
  }

  private static final Logger logger = LoggerFactory.getLogger(AbstractHttpOperator.class);
}
