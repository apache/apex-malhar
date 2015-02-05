/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator;
import com.datatorrent.lib.counters.BasicCounters;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.validation.constraints.NotNull;
import org.apache.commons.lang.mutable.MutableLong;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONObject;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class AppDataQueryAgent<T> implements Operator
{
  @NotNull
  private String supportedQuerySchemas;

  private MutableLong numReceivedQueries;
  private MutableLong numMalformedQueries;

  private transient OperatorContext context;
  private transient ObjectMapper mapper = new ObjectMapper();
  private transient Map<String, Class> schemaTypeToClass;
  private transient BasicCounters<MutableLong> appDataCounters = new BasicCounters<MutableLong>(MutableLong.class);

  public enum AppDataQueryCounters
  {
    RECEIVED_QUERIES,
    MALFORMED_QUERIES;
  }

  public AppDataQueryAgent()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    this.context = context;
    Map<String, String> schemaTypeToClassName;

    try {
      schemaTypeToClassName = mapper.readValue(supportedQuerySchemas, HashMap.class);
    }
    catch(IOException ex) {
      throw new RuntimeException(ex);
    }

    for(Map.Entry<String, String> entry: schemaTypeToClassName.entrySet()) {
      try {
        schemaTypeToClass.put(entry.getKey(), Class.forName(entry.getValue()));
      }
      catch(ClassNotFoundException ex) {
        throw new RuntimeException(ex);
      }
    }

    appDataCounters.setCounter(AppDataQueryCounters.RECEIVED_QUERIES, numReceivedQueries);
    appDataCounters.setCounter(AppDataQueryCounters.MALFORMED_QUERIES, numMalformedQueries);
  }

  public void setSupportedQuerySchemas(String supportedQuerySchemas)
  {
    this.supportedQuerySchemas = supportedQuerySchemas;
  }

  public String getSupportedQuerySchemas()
  {
    return supportedQuerySchemas;
  }

  @Override
  public void beginWindow(long windowId)
  {
    //Do nothing
  }

  public T convert(String jsonQuery)
  {
    //Get schema type
    JSONObject jsonObject = new JSONObject(jsonQuery);
    return null;
  }

  @Override
  public void endWindow()
  {
    if(context != null) {
      context.setCounters(appDataCounters);
    }
  }

  @Override
  public void teardown()
  {
    //Do nothing
  }
}
