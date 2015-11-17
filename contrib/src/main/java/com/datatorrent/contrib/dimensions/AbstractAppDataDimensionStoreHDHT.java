/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.dimensions;

import java.io.IOException;

import java.util.List;

import javax.validation.constraints.NotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.mutable.MutableLong;

import com.datatorrent.lib.appdata.StoreUtils;
import com.datatorrent.lib.appdata.query.QueryExecutor;
import com.datatorrent.lib.appdata.query.QueryManagerAsynchronous;
import com.datatorrent.lib.appdata.query.SimpleQueueManager;
import com.datatorrent.lib.appdata.query.serde.MessageDeserializerFactory;
import com.datatorrent.lib.appdata.query.serde.MessageSerializerFactory;
import com.datatorrent.lib.appdata.schemas.DataQueryDimensional;
import com.datatorrent.lib.appdata.schemas.Message;
import com.datatorrent.lib.appdata.schemas.Result;
import com.datatorrent.lib.appdata.schemas.ResultFormatter;
import com.datatorrent.lib.appdata.schemas.SchemaQuery;
import com.datatorrent.lib.appdata.schemas.SchemaRegistry;
import com.datatorrent.lib.appdata.schemas.SchemaResult;
import com.datatorrent.lib.dimensions.aggregator.AggregatorRegistry;
import com.datatorrent.lib.dimensions.aggregator.IncrementalAggregator;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.IdleTimeHandler;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

import com.datatorrent.common.experimental.AppData;
import com.datatorrent.common.experimental.AppData.EmbeddableQueryInfoProvider;

/**
 * This is a base class for App Data enabled Dimensions Stores. This class holds all the template code required
 * for processing AppData queries.
 * @since 3.1.0
 *
 */
public abstract class AbstractAppDataDimensionStoreHDHT extends DimensionsStoreHDHT implements IdleTimeHandler, AppData.Store<String>
{
  /**
   * This is the result formatter used to format data sent as a result to an App Data query.
   */
  @NotNull
  protected ResultFormatter resultFormatter = new ResultFormatter();
  /**
   * This is the {@link AggregatorRegistry} which holds the mapping from aggregator names and aggregator ids to
   * aggregators.
   */
  @NotNull
  protected AggregatorRegistry aggregatorRegistry = AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY;
  /**
   * This is the queue manager for schema queries.
   */
  protected transient SimpleQueueManager<SchemaQuery, Void, Void> schemaQueueManager;
  /**
   * This is the queue manager for data queries.
   */
  protected transient DimensionsQueueManager dimensionsQueueManager;
  /**
   * This is the query manager for schema queries.
   */
  protected transient QueryManagerAsynchronous<SchemaQuery, Void, Void, SchemaResult> schemaProcessor;
  /**
   * This is the query manager for data queries.
   */
  protected transient QueryManagerAsynchronous<DataQueryDimensional, QueryMeta, MutableLong, Result> queryProcessor;
  /**
   * This is the factory used to deserializes queries.
   */
  protected transient MessageDeserializerFactory queryDeserializerFactory;
  /**
   * This is the schema registry that holds all the schema information for the operator.
   */
  @VisibleForTesting
  public SchemaRegistry schemaRegistry;
  /**
   * This is the factory used to serialize results.
   */
  protected transient MessageSerializerFactory resultSerializerFactory;
  /**
   * Embeddable Query.
   */
  private EmbeddableQueryInfoProvider<String> embeddableQueryInfoProvider;

  private List<Message> dataMessages = Lists.newArrayList();

  private List<Message> schemaMessages = Lists.newArrayList();

  private transient boolean inWindow = false;

  /**
   * Optional unifier for query result port.
   */
  private Unifier<String> queryResultUnifier;

  public void setQueryResultUnifier(Unifier<String> queryResultUnifier)
  {
    this.queryResultUnifier = queryResultUnifier;
  }

  /**
   * This is the output port that serialized query results are emitted from.
   */
  @AppData.ResultPort
  public final transient DefaultOutputPort<String> queryResult = new DefaultOutputPort<String>()
  {
    @Override
    public Unifier<String> getUnifier()
    {
      if (AbstractAppDataDimensionStoreHDHT.this.queryResultUnifier == null) {
        return super.getUnifier();
      } else {
        return queryResultUnifier;
      }
    }
  };

  /**
   * This is the input port from which queries are received.
   */
  @InputPortFieldAnnotation(optional = true)
  @AppData.QueryPort
  public transient final DefaultInputPort<String> query = new DefaultInputPort<String>()
  {
    @Override
    public void process(String s)
    {
      LOG.debug("Received {}", s);

      //Deserialize a query
      Message query;
      try {
        query = queryDeserializerFactory.deserialize(s);
      }
      catch (IOException ex) {
        LOG.error("error parsing query {}", s, ex);
        return;
      }

      if (query instanceof SchemaQuery) {
        schemaMessages.add(query);

        //TODO this is a work around for APEX-129 and should be removed
        if (inWindow) {
          for (Message schemaMessage : schemaMessages) {
            //If the query is a SchemaQuery add it to the schemaQuery queue.
            schemaQueueManager.enqueue((SchemaQuery)schemaMessage, null, null);
          }

          schemaMessages.clear();
        }
      }
      else if (query instanceof DataQueryDimensional) {
        dataMessages.add(query);

        //TODO this is a work around for APEX-129 and should be removed
        if (inWindow) {
          for (Message dataMessage : dataMessages) {
            //If the query is a DataQueryDimensional add it to the dataQuery queue.
            dimensionsQueueManager.enqueue((DataQueryDimensional)dataMessage, null, null);
          }

          dataMessages.clear();
        }
      }
      else {
        LOG.warn("Invalid query {}", s);
      }
    }
  };

  /**
   * Constructor to create operator.
   */
  @SuppressWarnings("unchecked")
  public AbstractAppDataDimensionStoreHDHT()
  {
    //Do nothing
  }

  @Override
  public void activate(OperatorContext context)
  {
    if (embeddableQueryInfoProvider != null) {
      embeddableQueryInfoProvider.activate(context);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);

    aggregatorRegistry.setup();

    schemaRegistry = getSchemaRegistry();

    resultSerializerFactory = new MessageSerializerFactory(resultFormatter);

    queryDeserializerFactory = new MessageDeserializerFactory(SchemaQuery.class, DataQueryDimensional.class);
    queryDeserializerFactory.setContext(DataQueryDimensional.class, schemaRegistry);

    dimensionsQueueManager = getDimensionsQueueManager();
    queryProcessor
            = new QueryManagerAsynchronous<>(queryResult,
                                             dimensionsQueueManager,
                                             new DimensionsQueryExecutor(this, schemaRegistry),
                                             resultSerializerFactory,
                                             Thread.currentThread());

    schemaQueueManager = new SimpleQueueManager<>();
    schemaProcessor = new QueryManagerAsynchronous<>(queryResult,
                                                     schemaQueueManager,
                                                     new SchemaQueryExecutor(),
                                                     resultSerializerFactory,
                                                     Thread.currentThread());


    dimensionsQueueManager.setup(context);
    queryProcessor.setup(context);

    schemaQueueManager.setup(context);
    schemaProcessor.setup(context);

    if (embeddableQueryInfoProvider != null) {
      embeddableQueryInfoProvider.enableEmbeddedMode();
      LOG.info("An embeddable query operator is being used of class {}.", embeddableQueryInfoProvider.getClass().getName());
      StoreUtils.attachOutputPortToInputPort(embeddableQueryInfoProvider.getOutputPort(),
                                             query);
      embeddableQueryInfoProvider.setup(context);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    if (embeddableQueryInfoProvider != null) {
      embeddableQueryInfoProvider.beginWindow(windowId);
    }

    super.beginWindow(windowId);

    schemaQueueManager.beginWindow(windowId);
    schemaProcessor.beginWindow(windowId);

    dimensionsQueueManager.beginWindow(windowId);
    queryProcessor.beginWindow(windowId);

    inWindow = true;

    //TODO this is a work around for APEX-129 and should be removed
    for (Message schemaMessage : schemaMessages) {
      //If the query is a SchemaQuery add it to the schemaQuery queue.
      schemaQueueManager.enqueue((SchemaQuery)schemaMessage, null, null);
    }

    schemaMessages.clear();

    for (Message dataMessage : dataMessages) {
      //If the query is a DataQueryDimensional add it to the dataQuery queue.
      dimensionsQueueManager.enqueue((DataQueryDimensional)dataMessage, null, null);
    }

    dataMessages.clear();
  }

  @Override
  public void endWindow()
  {
    inWindow = false;

    if (embeddableQueryInfoProvider != null) {
      embeddableQueryInfoProvider.endWindow();
    }

    queryProcessor.endWindow();
    dimensionsQueueManager.endWindow();

    schemaProcessor.endWindow();
    schemaQueueManager.endWindow();

    super.endWindow();
  }

  @Override
  public void teardown()
  {
    if (embeddableQueryInfoProvider != null) {
      embeddableQueryInfoProvider.teardown();
    }

    queryProcessor.teardown();
    dimensionsQueueManager.teardown();

    schemaProcessor.teardown();
    schemaQueueManager.teardown();

    super.teardown();
  }

  @Override
  public void handleIdleTime()
  {
    //TODO this is a work around for APEX-129 and below should be uncommented
    //schemaProcessor.handleIdleTime();
    //queryProcessor.handleIdleTime();
  }

  @Override
  public void deactivate()
  {
    if (embeddableQueryInfoProvider != null) {
      embeddableQueryInfoProvider.deactivate();
    }
  }

  /**
   * Processes schema queries.
   * @param schemaQuery a schema query
   * @return The corresponding schema result.
   */
  protected abstract SchemaResult processSchemaQuery(SchemaQuery schemaQuery);

  /**
   * Gets the {@link SchemaRegistry} used by this operator.
   * @return The {@link SchemaRegistry} used by this operator.
   */
  protected abstract SchemaRegistry getSchemaRegistry();

  protected DimensionsQueueManager getDimensionsQueueManager()
  {
    return new DimensionsQueueManager(this, schemaRegistry);
  }

  @Override
  public IncrementalAggregator getAggregator(int aggregatorID)
  {
    return aggregatorRegistry.getIncrementalAggregatorIDToAggregator().get(aggregatorID);
  }

  @Override
  protected int getAggregatorID(String aggregatorName)
  {
    return aggregatorRegistry.getIncrementalAggregatorNameToID().get(aggregatorName);
  }

  /**
   * Sets the {@link ResultFormatter} to use on App Data results emitted by this operator.
   * @param resultFormatter The {@link ResultFormatter} to use on App Data results emitted
   * by this operator.
   */
  public void setResultFormatter(ResultFormatter resultFormatter)
  {
    this.resultFormatter = resultFormatter;
  }

  /**
   * Returns the {@link ResultFormatter} to use on App Data results emitted by this operator.
   * @return The {@link ResultFormatter} to use on App Data results emitted by this operator.
   */
  public ResultFormatter getResultFormatter()
  {
    return resultFormatter;
  }

  /**
   * Returns the {@link AggregatorRegistry} used by this operator.
   * @return The {@link AggregatorRegistry} used by this operator.
   */
  protected AggregatorRegistry getAggregatorRegistry()
  {
    return aggregatorRegistry;
  }

  /**
   * Sets the {@link AggregatorRegistry} used by this operator.
   * @param aggregatorRegistry The {@link AggregatorRegistry} used by this operator.
   */
  public void setAggregatorRegistry(@NotNull AggregatorRegistry aggregatorRegistry)
  {
    this.aggregatorRegistry = aggregatorRegistry;
  }

  @Override
  public EmbeddableQueryInfoProvider<String> getEmbeddableQueryInfoProvider()
  {
    return embeddableQueryInfoProvider;
  }

  @Override
  public void setEmbeddableQueryInfoProvider(EmbeddableQueryInfoProvider<String> embeddableQueryInfoProvider)
  {
    this.embeddableQueryInfoProvider = embeddableQueryInfoProvider;
  }

  /**
   * This is a {@link QueryExecutor} that is responsible for executing schema queries.
   */
  public class SchemaQueryExecutor implements QueryExecutor<SchemaQuery, Void, Void, SchemaResult>
  {
    /**
     * Creates a {@link SchemaQueryExecutor}
     */
    public SchemaQueryExecutor()
    {
      //Do nothing
    }

    @Override
    public SchemaResult executeQuery(SchemaQuery query, Void metaQuery, Void queueContext)
    {
      return processSchemaQuery(query);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(AbstractAppDataDimensionStoreHDHT.class);
}
