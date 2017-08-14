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

import com.google.common.base.Preconditions;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.experimental.AppData;

/**
 * This is an operator that lays the framework of serving data to queries coming from an embeddable query info provider,
 * which may be an input operator.
 * Subclasses are expected to implement the processQuery method for the logic of handling the query. Note that
 * processQuery cannot directly emit to the operator's output port because it's called from the thread of the
 * embeddable query info provider.
 *
 * @since 3.8.0
 */
public abstract class AbstractAppDataServer<QueryType> implements Operator, AppData.Store<QueryType>
{
  @AppData.QueryPort
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<QueryType> query = new DefaultInputPort<QueryType>()
  {
    @Override
    public void process(QueryType query)
    {
      processQuery(query);
    }
  };


  protected AppData.EmbeddableQueryInfoProvider<QueryType> embeddableQueryInfoProvider;


  protected abstract void processQuery(QueryType query);

  @Override
  public void activate(Context.OperatorContext ctx)
  {
    if (embeddableQueryInfoProvider != null) {
      embeddableQueryInfoProvider.activate(ctx);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void setup(Context.OperatorContext context)
  {
    if (embeddableQueryInfoProvider != null) {
      embeddableQueryInfoProvider.enableEmbeddedMode();
      LOG.info("An embeddable query operator is being used of class {}.", embeddableQueryInfoProvider.getClass().getName());
      StoreUtils.attachOutputPortToInputPort(embeddableQueryInfoProvider.getOutputPort(), query);
      embeddableQueryInfoProvider.setup(context);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    if (embeddableQueryInfoProvider != null) {
      embeddableQueryInfoProvider.beginWindow(windowId);
    }
  }

  @Override
  public void endWindow()
  {
    if (embeddableQueryInfoProvider != null) {
      embeddableQueryInfoProvider.endWindow();
    }
  }

  @Override
  public void teardown()
  {
    if (embeddableQueryInfoProvider != null) {
      embeddableQueryInfoProvider.teardown();
    }
  }

  @Override
  public void deactivate()
  {
    if (embeddableQueryInfoProvider != null) {
      embeddableQueryInfoProvider.deactivate();
    }
  }

  @Override
  public AppData.EmbeddableQueryInfoProvider<QueryType> getEmbeddableQueryInfoProvider()
  {
    return embeddableQueryInfoProvider;
  }

  @Override
  public void setEmbeddableQueryInfoProvider(AppData.EmbeddableQueryInfoProvider<QueryType> embeddableQueryInfoProvider)
  {
    this.embeddableQueryInfoProvider = Preconditions.checkNotNull(embeddableQueryInfoProvider);
  }

  private static final Logger LOG = LoggerFactory.getLogger(AbstractAppDataServer.class);

}
