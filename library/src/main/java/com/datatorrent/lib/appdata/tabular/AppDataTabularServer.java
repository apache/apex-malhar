/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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

package com.datatorrent.lib.appdata.tabular;

import com.datatorrent.lib.appdata.query.serde.Result;
import com.datatorrent.lib.appdata.query.serde.Message;
import com.datatorrent.lib.appdata.query.serde.MessageSerializerFactory;
import com.datatorrent.lib.appdata.query.serde.MessageDeserializerFactory;
import com.datatorrent.lib.appdata.query.serde.Query;
import java.io.IOException;

import java.util.List;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import com.datatorrent.api.AppData;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.query.AppDataWindowEndQueueManager;
import com.datatorrent.lib.appdata.query.QueryExecutor;
import com.datatorrent.lib.appdata.query.QueryManager;
import com.datatorrent.lib.appdata.schemas.*;

public abstract class AppDataTabularServer<INPUT_EVENT> implements Operator
{
  private static final Logger logger = LoggerFactory.getLogger(AppDataTabularServer.class);

  private transient QueryManager<Query, Void, MutableLong, Result> queryProcessor;
  private transient MessageDeserializerFactory queryDeserializerFactory;
  private transient MessageSerializerFactory resultSerializerFactory;
  private transient SchemaRegistry schemaRegistry;
  protected transient SchemaTabular schema;

  @NotNull
  private ResultFormatter resultFormatter = new ResultFormatter();
  private String tabularSchemaJSON;
  private List<GPOMutable> currentData = Lists.newArrayList();

  @AppData.ResultPort
  public final transient DefaultOutputPort<String> queryResult = new DefaultOutputPort<String>();

  @AppData.QueryPort
  public transient final DefaultInputPort<String> query = new DefaultInputPort<String>()
  {
    @Override
    public void process(String queryJSON)
    {
      Message query = null;

      try {
        query = queryDeserializerFactory.deserialize(queryJSON);
      }
      catch(IOException ex) {
        logger.error("Error parsing query: {}", queryJSON);
        logger.error("{}", ex);
        return;
      }

      if(query instanceof SchemaQuery) {
        SchemaResult schemaResult = schemaRegistry.getSchemaResult((SchemaQuery) query);

        if(schemaResult != null) {
          String schemaResultJSON = resultSerializerFactory.serialize(schemaResult);
          queryResult.emit(schemaResultJSON);
        }
      }
      else if(query instanceof DataQueryTabular) {
        queryProcessor.enqueue((DataQueryTabular) query, null, null);
      }
    }
  };

  public transient final DefaultInputPort<List<INPUT_EVENT>> input = new DefaultInputPort<List<INPUT_EVENT>>()
  {
    @Override
    public void process(List<INPUT_EVENT> rows)
    {
      currentData.clear();

      for(INPUT_EVENT inputEvent: rows) {
        GPOMutable gpoRow = convert(inputEvent);
        currentData.add(gpoRow);
      }
    }
  };

  public AppDataTabularServer()
  {
  }

  public abstract GPOMutable convert(INPUT_EVENT inputEvent);

  @SuppressWarnings("unchecked")
  @Override
  public void setup(OperatorContext context)
  {
    schema = new SchemaTabular(tabularSchemaJSON);
    schemaRegistry = new SchemaRegistrySingle(schema);
    //Setup for query processing
    queryProcessor = QueryManager.newInstance(new TabularComputer(), new AppDataWindowEndQueueManager<Query, Void>());

    queryDeserializerFactory = new MessageDeserializerFactory(SchemaQuery.class,
                                                           DataQueryTabular.class);
    queryDeserializerFactory.setContext(DataQueryTabular.class, schemaRegistry);
    resultSerializerFactory = new MessageSerializerFactory(resultFormatter);
    queryProcessor.setup(context);
  }

  @Override
  public void beginWindow(long windowId)
  {
    queryProcessor.beginWindow(windowId);
  }

  @Override
  public void endWindow()
  {
    {
      Result result = null;

      while((result = queryProcessor.process()) != null) {
        queryResult.emit(resultSerializerFactory.serialize(result));
      }
    }

    queryProcessor.endWindow();
  }

  @Override
  public void teardown()
  {
    queryProcessor.teardown();
  }

  /**
   * @return the tabularSchemaJSON
   */
  public String getTabularSchemaJSON()
  {
    return tabularSchemaJSON;
  }

  /**
   * @param tabularSchemaJSON the tabularSchemaJSON to set
   */
  public void setTabularSchemaJSON(String tabularSchemaJSON)
  {
    this.tabularSchemaJSON = tabularSchemaJSON;
  }

  /**
   * @return the resultFormatter
   */
  public ResultFormatter getResultFormatter()
  {
    return resultFormatter;
  }

  /**
   * @param resultFormatter the resultFormatter to set
   */
  public void setResultFormatter(ResultFormatter resultFormatter)
  {
    this.resultFormatter = resultFormatter;
  }

  public class TabularComputer implements QueryExecutor<Query, Void, MutableLong, Result>
  {
    @Override
    public Result executeQuery(Query query, Void metaQuery, MutableLong queueContext)
    {
      return new DataResultTabular(query,
                                   currentData);
    }
  }
}
