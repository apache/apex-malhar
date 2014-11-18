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
package com.datatorrent.demos.goldengate;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

import com.datatorrent.common.util.DTThrowable;

/**
 * This operator processes queries.
 */
public abstract class QueryProcessor extends BaseOperator
{
  protected transient ObjectMapper mapper;
  private transient Map<Query, Long> queries = new HashMap<Query, Long>();
  private transient long currentWindowId;
  private long queryExpiryWindows = 30;
  private transient FileSystem fs;
  @OutputPortFieldAnnotation()
  public final transient DefaultOutputPort<QueryResults> queryOutput = new DefaultOutputPort<QueryResults>();

  @Override
  public void setup(OperatorContext context)
  {
    try {
      fs = FileSystem.newInstance(new Configuration());
      mapper = new ObjectMapper();
      mapper.configure(org.codehaus.jackson.map.DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;
  }


  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<String> queryInput = new DefaultInputPort<String>()
  {
    @Override
    public void process(String t)
    {
      processQuery(t);
    }

  };

  @SuppressWarnings("unchecked")
  private void processQuery(String queryString)
  {
    logger.debug("process query {}", queryString);
    try {
      JsonNode json = mapper.readTree(queryString);
      JsonNode idJson = json.get("id");
      JsonNode keys = json.get("keys");
      if ((idJson != null) && (keys != null)) {
        String id = idJson.getTextValue();
        JsonNode selectorJson = keys.get("selector");
        if (selectorJson != null) {
          String selector = selectorJson.getTextValue();
          Class<? extends Query> queryClass = getQueryClass(selector, keys);
          if (queryClass != null) {
            Query query = mapper.readValue(keys, queryClass);
            query.id = id;
            setQueryProperties(query, keys);
            processQuery(query);
          }
        }
      }
    }
    catch (Exception ex) {
      DTThrowable.rethrow(ex);
    }

  }

  protected void processQuery(Query query)
  {
    if (!query.oneTime) {
      registerQuery(query);
    } else {
      executeQueryWithResponse(query);
    }
  }

  protected void executeQueryWithResponse(Query query) {
    QueryResults results = new QueryResults();
    results.setId(query.id);
    executeQuery(query, results);
    queryOutput.emit(results);
  }

  protected void setQueryProperties(Query query, JsonNode json) {
  }

  protected abstract Class<? extends Query> getQueryClass(String selector, JsonNode json);

  protected abstract void executeQuery(Query query, QueryResults results);

  private void registerQuery(Query query)
  {
    if (query != null) {
      queries.put(query, currentWindowId);
    }
  }

  @Override
  public void endWindow()
  {
    if (queryOutput.isConnected()) {
      emitQueryResults();
    }
  }

  private void emitQueryResults()
  {
    for (Iterator<Map.Entry<Query, Long>> it = queries.entrySet().iterator(); it.hasNext();) {
      Entry<Query, Long> entry = it.next();
      Query query = entry.getKey();
      Long windowId = entry.getValue();

      if (currentWindowId - windowId > queryExpiryWindows) {
        // removing expired queries
        it.remove();
      }
      else {
        executeQueryWithResponse(query);
      }

    }
  }

  public static class Query
  {
    public String selector;
    public int numResults;
    public String id;
    @JsonIgnore
    public transient boolean oneTime = false;

    @Override
    public int hashCode()
    {
      int hash = 3;
      hash = 43 * hash + (this.id != null ? this.id.hashCode() : 0);
      return hash;
    }

    @Override
    public boolean equals(Object obj)
    {
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final Query other = (Query)obj;
      if ((this.id == null) ? (other.id != null) : !this.id.equals(other.id)) {
        return false;
      }
      return true;
    }

    @Override
    public String toString()
    {
      return "Query{" + "selector=" + selector + ", numResults=" + numResults + ", id=" + id + '}';
    }

  }

  public static class QueryResults
  {
    String id = "DEFAULT";
    String type;
    Data data;

    public String getType()
    {
      return type;
    }

    public void setType(String type)
    {
      this.type = type;
    }

    public Data getData()
    {
      return data;
    }

    public void setData(Data data)
    {
      this.data = data;
    }

    public String getId()
    {
      return id;
    }

    public void setId(String id)
    {
      this.id = id;
    }

    public static interface Data
    {
    }

    @Override
    public String toString()
    {
      return "QueryResults [id=" + id + ", type=" + type + ", data=" + data + "]";
    }

  }

  private static final Logger logger = LoggerFactory.getLogger(QueryProcessor.class);
}
