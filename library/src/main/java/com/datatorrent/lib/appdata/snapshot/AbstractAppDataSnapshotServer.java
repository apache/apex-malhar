/*
 * Copyright (c) 2015 DataTorrent, Inc.
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
package com.datatorrent.lib.appdata.snapshot;

import java.io.IOException;

import java.util.List;

import javax.validation.constraints.NotNull;

import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.mutable.MutableLong;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.query.AppDataWindowEndQueueManager;
import com.datatorrent.lib.appdata.query.QueryExecutor;
import com.datatorrent.lib.appdata.query.QueryManagerSynchronous;
import com.datatorrent.lib.appdata.query.serde.MessageDeserializerFactory;
import com.datatorrent.lib.appdata.query.serde.MessageSerializerFactory;
import com.datatorrent.lib.appdata.schemas.*;
import com.datatorrent.lib.appdata.schemas.Message;
import com.datatorrent.lib.appdata.schemas.Query;
import com.datatorrent.lib.appdata.schemas.Result;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;

import com.datatorrent.common.experimental.AppData;

/**
 * This is an abstract operator for the {@link SnapshotSchema}. This operator is designed to accept input data
 * in the form of a list of objects. The last list of data sent to the operator is what the operator will serve.
 * Additionally the list of input objects then need to be converted into {@link GPOMutable} objects
 * via an implementation of the {@link #convert} convert method.
 * @param <INPUT_EVENT> The type of the input events that the operator accepts.
 */
public abstract class AbstractAppDataSnapshotServer<INPUT_EVENT> implements Operator
{
  /**
   * The {@link QueryManagerSynchronous} for the operator.
   */
  private transient QueryManagerSynchronous<Query, Void, MutableLong, Result> queryProcessor;
  /**
   * The {@link MessageDeserializerFactory} for the operator.
   */
  private transient MessageDeserializerFactory queryDeserializerFactory;
  /**
   * The {@link MessageSerializerFactory} for the operator.
   */
  private transient MessageSerializerFactory resultSerializerFactory;
  /**
   * The {@link SchemaRegistry} for the operator.
   */
  private transient SchemaRegistry schemaRegistry;
  /**
   * The schema for the operator.
   */
  protected transient SnapshotSchema schema;

  @NotNull
  private ResultFormatter resultFormatter = new ResultFormatter();
  private String snapshotSchemaJSON;
  /**
   * The current data to be served by the operator.
   */
  private List<GPOMutable> currentData = Lists.newArrayList();

  @AppData.ResultPort
  public final transient DefaultOutputPort<String> queryResult = new DefaultOutputPort<String>();

  @AppData.QueryPort
  public transient final DefaultInputPort<String> query = new DefaultInputPort<String>()
  {
    @Override
    public void process(String queryJSON)
    {
      LOG.debug("query {}", queryJSON);
      Message query = null;

      try {
        query = queryDeserializerFactory.deserialize(queryJSON);
      }
      catch(IOException ex) {
        LOG.error("Error parsing query: {}", queryJSON);
        LOG.error("{}", ex);
        return;
      }

      if(query instanceof SchemaQuery) {
        SchemaResult schemaResult = schemaRegistry.getSchemaResult((SchemaQuery) query);

        if(schemaResult != null) {
          String schemaResultJSON = resultSerializerFactory.serialize(schemaResult);
          LOG.debug("emitting {}", schemaResultJSON);
          queryResult.emit(schemaResultJSON);
        }
      }
      else if(query instanceof DataQuerySnapshot) {
        queryProcessor.enqueue((DataQuerySnapshot) query, null, null);
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

  /**
   * Create operator.
   */
  public AbstractAppDataSnapshotServer()
  {
    //Do nothing
  }

  /**
   * This method converts input data to GPOMutable objects to serve.
   * @param inputEvent The input object to convert to a {@link GPOMutable{.
   * @return The converted input event.
   */
  public abstract GPOMutable convert(INPUT_EVENT inputEvent);

  @SuppressWarnings("unchecked")
  @Override
  public void setup(OperatorContext context)
  {
    schema = new SnapshotSchema(snapshotSchemaJSON);
    schemaRegistry = new SchemaRegistrySingle(schema);
    //Setup for query processing
    queryProcessor = QueryManagerSynchronous.newInstance(new SnapshotComputer(), new AppDataWindowEndQueueManager<Query, Void>());

    queryDeserializerFactory = new MessageDeserializerFactory(SchemaQuery.class,
                                                           DataQuerySnapshot.class);
    queryDeserializerFactory.setContext(DataQuerySnapshot.class, schemaRegistry);
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
        String resultJSON = resultSerializerFactory.serialize(result);
        LOG.debug("emitting {}", resultJSON);
        queryResult.emit(resultJSON);
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
   * Gets the JSON for the schema.
   * @return the JSON for the schema.
   */
  public String getSnapshotSchemaJSON()
  {
    return snapshotSchemaJSON;
  }

  /**
   * Sets the JSON for the schema.
   * @param snapshotSchemaJSON The JSON for the schema.
   */
  public void setSnapshotSchemaJSON(String snapshotSchemaJSON)
  {
    this.snapshotSchemaJSON = snapshotSchemaJSON;
  }

  /**
   * Gets the {@link ResultFormatter} for the data.
   * @return The {@link ResultFormatter} for the data.
   */
  public ResultFormatter getResultFormatter()
  {
    return resultFormatter;
  }

  /**
   * Sets the {@link ResultFormatter} for the data.
   * @param resultFormatter The {@link ResultFormatter} for the data.
   */
  public void setResultFormatter(ResultFormatter resultFormatter)
  {
    this.resultFormatter = resultFormatter;
  }

  /**
   * The {@link QueryExecutor} which returns the results for queries.
   */
  public class SnapshotComputer implements QueryExecutor<Query, Void, MutableLong, Result>
  {
    @Override
    public Result executeQuery(Query query, Void metaQuery, MutableLong queueContext)
    {
      return new DataResultSnapshot(query,
                                   currentData,
                                   queueContext.getValue());
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(AbstractAppDataSnapshotServer.class);
}
