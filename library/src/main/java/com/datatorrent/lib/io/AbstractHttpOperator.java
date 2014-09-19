/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.io;

import javax.validation.constraints.NotNull;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;

/**
 * This is an abstract http operator that creates a client at setup and destroys it teardown.&nbsp;
 * It also provides the process tuple method to be implemented to process each incoming tuple.
 * <p></p>
 * @displayName Abstract Http
 * @category io
 * @tags http, input operator
 *
 * @param <T>
 * @since 1.0.2
 */
public abstract class AbstractHttpOperator<T> extends BaseOperator
{
  @NotNull
  protected String url;
  protected transient Client wsClient;
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
