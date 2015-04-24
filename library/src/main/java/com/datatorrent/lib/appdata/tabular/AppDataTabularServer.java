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

import com.datatorrent.api.AppData;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.qr.Data;
import com.datatorrent.lib.appdata.qr.DataDeserializerFactory;
import com.datatorrent.lib.appdata.qr.DataSerializerFactory;
import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.Result;
import com.datatorrent.lib.appdata.qr.processor.AppDataWWEQueryQueueManager;
import com.datatorrent.lib.appdata.qr.processor.QueryComputer;
import com.datatorrent.lib.appdata.qr.processor.QueryProcessor;
import com.datatorrent.lib.appdata.schemas.AppDataFormatter;
import com.datatorrent.lib.appdata.schemas.DataQueryTabular;
import com.datatorrent.lib.appdata.schemas.DataResultTabular;
import com.datatorrent.lib.appdata.schemas.SchemaQuery;
import com.datatorrent.lib.appdata.schemas.SchemaResult;
import com.datatorrent.lib.appdata.schemas.SchemaTabular;
import com.google.common.collect.Lists;
import javax.validation.constraints.NotNull;
import org.apache.commons.lang3.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class AppDataTabularServer<INPUT_EVENT> implements Operator
{
  private static final Logger logger = LoggerFactory.getLogger(AppDataTabularServer.class);

  private transient QueryProcessor<Query, Void, MutableLong, Void, Result> queryProcessor;
  private transient DataDeserializerFactory queryDeserializerFactory;
  private transient DataSerializerFactory resultSerializerFactory;
  @NotNull
  private AppDataFormatter appDataFormatter = new AppDataFormatter();

  private String tabularSchemaJSON;
  protected SchemaTabular schema;

  private List<GPOMutable> currentData = Lists.newArrayList();

  @AppData.ResultPort
  public final transient DefaultOutputPort<String> queryResult = new DefaultOutputPort<String>();

  @AppData.QueryPort
  public transient final DefaultInputPort<String> query = new DefaultInputPort<String>()
  {
    @Override
    public void process(String queryJSON)
    {
      logger.info("Received: {}", queryJSON);

      Data query = queryDeserializerFactory.deserialize(queryJSON);

      //Query was not parseable
      if(query == null) {
        logger.info("Not parseable.");
        return;
      }

      if(query instanceof SchemaQuery) {
        String schemaResult = resultSerializerFactory.serialize(new SchemaResult((SchemaQuery) query,
                                                                                  schema));
        queryResult.emit(schemaResult);
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

  @Override
  public void setup(OperatorContext context)
  {
    schema = new SchemaTabular(tabularSchemaJSON);

    //Setup for query processing
    queryProcessor = new QueryProcessor<Query, Void, MutableLong, Void, Result>(
                     new TabularComputer(),
                     new AppDataWWEQueryQueueManager<Query, Void>());

    queryDeserializerFactory = new DataDeserializerFactory(SchemaQuery.class,
                                                           DataQueryTabular.class);
    queryDeserializerFactory.setContext(DataQueryTabular.class, schema);
    resultSerializerFactory = new DataSerializerFactory(appDataFormatter);
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

      while((result = queryProcessor.process(null)) != null) {
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
   * @return the appDataFormatter
   */
  public AppDataFormatter getAppDataFormatter()
  {
    return appDataFormatter;
  }

  /**
   * @param appDataFormatter the appDataFormatter to set
   */
  public void setAppDataFormatter(AppDataFormatter appDataFormatter)
  {
    this.appDataFormatter = appDataFormatter;
  }

  public class TabularComputer implements QueryComputer<Query, Void, MutableLong, Void, Result>
  {
    @Override
    public Result processQuery(Query query, Void metaQuery, MutableLong queueContext, Void context)
    {
      return new DataResultTabular(query,
                                   currentData);
    }

    @Override
    public void queueDepleted(Void context)
    {
    }
  }
}
