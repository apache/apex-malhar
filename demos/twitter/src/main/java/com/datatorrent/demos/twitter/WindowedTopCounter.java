/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.demos.twitter;

import java.util.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;

import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.mutable.MutableLong;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.qr.Data;
import com.datatorrent.lib.appdata.qr.DataDeserializerFactory;
import com.datatorrent.lib.appdata.qr.DataSerializerFactory;
import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.Result;
import com.datatorrent.lib.appdata.qr.processor.AppDataWWEQueryQueueManager;
import com.datatorrent.lib.appdata.qr.processor.QueryComputer;
import com.datatorrent.lib.appdata.qr.processor.QueryProcessor;
import com.datatorrent.lib.appdata.schemas.GenericDataQueryTabular;
import com.datatorrent.lib.appdata.schemas.GenericDataResultTabular;
import com.datatorrent.lib.appdata.schemas.GenericSchemaResult;
import com.datatorrent.lib.appdata.schemas.GenericSchemaTabular;
import com.datatorrent.lib.appdata.schemas.SchemaQuery;

import com.datatorrent.api.*;
import com.datatorrent.api.Context.OperatorContext;

/**
 *
 * WindowedTopCounter is an operator which counts the most often occurring tuples in a sliding window of a specific size.
 * The operator expects to receive a map object which contains a set of objects mapped to their respective frequency of
 * occurrences. e.g. if we are looking at most commonly occurring names then the operator expects to receive the tuples
 * of type Map<String, Intenger> on its input port, and at the end of the window it emits 1 object of type Map<String, Integer>
 * with a pre determined size. The emitted object contains the most frequently occurring keys.
 *
 * @param <T> Type of the key in the map object which is accepted on input port as payload. Note that this key must be HashMap friendly.
 * @since 0.3.2
 */
public class WindowedTopCounter<T> extends BaseOperator
{
  public static final String FIELD_URL = "url";
  public static final String FIELD_COUNT = "count";

  private static final Logger logger = LoggerFactory.getLogger(WindowedTopCounter.class);

  //==========================================================================
  // Query Processing - Start
  //==========================================================================

  private transient QueryProcessor<Query, Void, MutableLong, Void, Result> queryProcessor;
  @SuppressWarnings("unchecked")
  private transient DataDeserializerFactory queryDeserializerFactory;
  private transient DataSerializerFactory resultSerializerFactory;

  private String schemaJSON;
  private transient GenericSchemaTabular schema;

  //==========================================================================
  // Query Processing - End
  //==========================================================================

  private PriorityQueue<SlidingContainer<T>> topCounter;
  private int windows;
  private int topCount = 10;
  private HashMap<T, SlidingContainer<T>> objects = new HashMap<T, SlidingContainer<T>>();

  @AppData.ResultPort(schemaType = "default", schemaVersion = "1.0")
  public final transient DefaultOutputPort<String> resultOutput = new DefaultOutputPort<String>();

  /**
   * Input port on which map objects containing keys with their respective frequency as values will be accepted.
   */
  public final transient DefaultInputPort<Map<T, Integer>> input = new DefaultInputPort<Map<T, Integer>>()
  {
    @Override
    public void process(Map<T, Integer> map)
    {
      for (Map.Entry<T, Integer> e : map.entrySet()) {
        SlidingContainer<T> holder = objects.get(e.getKey());
        if (holder == null) {
          holder = new SlidingContainer<T>(e.getKey(), windows);
          objects.put(e.getKey(), holder);
        }
        holder.adjustCount(e.getValue());
      }
    }
  };

  @AppData.QueryPort
  public final transient DefaultInputPort<String> queryInput = new DefaultInputPort<String>() {
    @Override
    public void process(String s)
    {
      logger.info("Received: {}", s);

      Data query = queryDeserializerFactory.deserialize(s);

      //Query was not parseable
      if(query == null) {
        logger.info("Not parseable.");
        return;
      }

      if(query instanceof SchemaQuery) {
        String schemaResult = resultSerializerFactory.serialize(new GenericSchemaResult((SchemaQuery) query,
                                                                                        schema));
        resultOutput.emit(schemaResult);
      }
      else if(query instanceof GenericDataQueryTabular) {
        queryProcessor.enqueue((GenericDataQueryTabular) query, null, null);
      }
    }
  };

  /**
   * Set the width of the sliding window.
   *
   * Sliding window is typically much larger than the dag window. e.g. One may want to measure the most frequently
   * occurring keys over the period of 5 minutes. So if dagWindowWidth (which is by default 500ms) is set to 500ms,
   * the slidingWindowWidth would be (60 * 5 * 1000 =) 300000.
   *
   * @param slidingWindowWidth - Sliding window width to be set for this operator, recommended to be multiple of DAG window.
   * @param dagWindowWidth - DAG's native window width. It has to be the value of the native window set at the application level.
   */
  public void setSlidingWindowWidth(long slidingWindowWidth, int dagWindowWidth)
  {
    windows = (int)(slidingWindowWidth / dagWindowWidth) + 1;
    if (slidingWindowWidth % dagWindowWidth != 0) {
      logger.warn("slidingWindowWidth(" + slidingWindowWidth + ") is not exact multiple of dagWindowWidth(" + dagWindowWidth + ")");
    }
  }

  @Override
  public void setup(OperatorContext context)
  {
    topCounter = new PriorityQueue<SlidingContainer<T>>(this.topCount, new TopSpotComparator());

    schema = new GenericSchemaTabular(schemaJSON);

    //Setup for query processing
    queryProcessor = new QueryProcessor<Query, Void, MutableLong, Void, Result>(
                     new WindowTopCounterComputer(),
                     new AppDataWWEQueryQueueManager<Query, Void>());

    queryDeserializerFactory = new DataDeserializerFactory(SchemaQuery.class,
                                                           GenericDataQueryTabular.class);
    queryDeserializerFactory.setContext(GenericDataQueryTabular.class, schema);

    resultSerializerFactory = new DataSerializerFactory();
    queryProcessor.setup(context);
  }

  @Override
  public void beginWindow(long windowId)
  {
    topCounter.clear();
    queryProcessor.beginWindow(windowId);
  }

  @Override
  public void endWindow()
  {
    Iterator<Map.Entry<T, SlidingContainer<T>>> iterator = objects.entrySet().iterator();
    int i = topCount;

    /*
     * Try to fill the priority queue with the first topCount URLs.
     */
    SlidingContainer<T> holder;
    while (iterator.hasNext()) {
      holder = iterator.next().getValue();
      holder.slide();

      if (holder.totalCount == 0) {
        iterator.remove();
      }
      else {
        topCounter.add(holder);
        if (--i == 0) {
          break;
        }
      }
    }
    logger.debug("objects.size(): {}", objects.size());

    /*
     * Make room for the new element in the priority queue by deleting the
     * smallest one, if we KNOW that the new element is useful to us.
     */
    if (i == 0) {
      int smallest = topCounter.peek().totalCount;
      while (iterator.hasNext()) {
        holder = iterator.next().getValue();
        holder.slide();

        if (holder.totalCount > smallest) {
          topCounter.poll();
          topCounter.add(holder);
          smallest = topCounter.peek().totalCount;
        }
        else if (holder.totalCount == 0) {
          iterator.remove();
        }
      }
    }

    {
      Result result = null;

      while((result = queryProcessor.process(null)) != null) {
        resultOutput.emit(resultSerializerFactory.serialize(result));
      }
    }

    queryProcessor.endWindow();
    topCounter.clear();
  }

  @Override
  public void teardown()
  {
    topCounter = null;
    objects = null;
    queryProcessor.teardown();
  }

  /**
   * Set the count of most frequently occurring keys to emit per map object.
   *
   * @param count count of the objects in the map emitted at the output port.
   */
  public void setTopCount(int count)
  {
    topCount = count;
  }

  /**
   * @return the schemaJSON
   */
  public String getSchemaJSON()
  {
    return schemaJSON;
  }

  /**
   * @param schemaJSON the schemaJSON to set
   */
  public void setDataSchema(String schemaJSON)
  {
    this.schemaJSON = schemaJSON;
  }

  class WindowTopCounterComputer implements QueryComputer<Query, Void, MutableLong, Void, Result>
  {
    @Override
    public Result processQuery(Query query, Void metaQuery, MutableLong queueContext, Void context)
    {
      GenericDataQueryTabular gQuery = (GenericDataQueryTabular) query;
      List<GPOMutable> data = Lists.newArrayList();

      Iterator<SlidingContainer<T>> topIter = topCounter.iterator();

      while(topIter.hasNext()) {
        final SlidingContainer<T> wh = topIter.next();

        GPOMutable dataPoint = new GPOMutable(schema.getValuesDescriptor());

        dataPoint.setField(FIELD_URL, wh.identifier.toString());
        dataPoint.setField(FIELD_COUNT, wh.totalCount);

        data.add(dataPoint);
      }

      return new GenericDataResultTabular(gQuery,
                                          data,
                                          queueContext.longValue());
    }

    @Override
    public void queueDepleted(Void context)
    {
    }
  }

  static class TopSpotComparator implements Comparator<SlidingContainer<?>>
  {
    @Override
    public int compare(SlidingContainer<?> o1, SlidingContainer<?> o2)
    {
      if (o1.totalCount > o2.totalCount) {
        return 1;
      }
      else if (o1.totalCount < o2.totalCount) {
        return -1;
      }

      return 0;
    }
  }
}
