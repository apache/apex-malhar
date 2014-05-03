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
package com.datatorrent.apps.etl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.python.google.common.collect.Lists;

/**
 * Abstract chart class used to define various charts
 */
public abstract class Chart
{
  public enum CHART_TYPE
  {
    LINE, MAP, TOP
  }

  public String name;
  public CHART_TYPE type;

  public abstract Object getMeta();

  /**
   * Chart parameters corresponding to each line chart
   */
  public static class LineChartParams extends Chart
  {
    public String xDimensionKey; // eg: timestamp
    public String xDimensionUnit; // eg: MINUTE
    public int xNumPoints; // eg: 15
    @NotNull
    public List<String> metrics; // eg: sum, count, avg
    public Set<String> dimensions; // eg: bucket, country
    public Map<String, String> filters; // eg: bucket = MINUTE, country = US
    public String[] params;
    public final String X_KEY = "xKey";
    public final String X_UNIT = "xUnit";
    public final String FILTERS = "filters";
    public final String NAME = "name";
    public final String TYPE = "type";
    public final String Y_KEYS = "yKeys";
    public final String SCHEMA = "schema";

    public LineChartParams()
    {
    }

    public void init(String[] params)
    {
      this.params = params;
      //eg: line, timestamp, MINUTE:30, country=US:url=/home, sum:count:avg:avgRespTime
      //eg: line, numUsers, 100:20,  country=US:url=/home, avgRespTime
      dimensions = Sets.newTreeSet();
      filters = Maps.newHashMap();

      // chart type
      type = CHART_TYPE.valueOf(params[0]);

      // x axis
      xDimensionKey = params[1];
      dimensions.add(xDimensionKey);
      if (xDimensionKey.equalsIgnoreCase(Constants.TIME_ATTR)) {
        dimensions.add("bucket");
      }
      String[] x = params[2].split(":");
      xDimensionUnit = x[0];
      xNumPoints = Integer.parseInt(x[1]);

      // filter
      if (params[3] != null && !params[3].isEmpty()) {
        String[] filerList = params[3].split(":");
        for (String elem : filerList) {
          String[] fil = elem.split("=");
          filters.put(fil[0], fil[1]);
          dimensions.add(fil[0]);
        }
      }

      // metrics
      String[] metricList = params[4].split(":");
      metrics = Lists.newArrayList(metricList);
    }

    @Override
    public String toString()
    {
      return "LineChartParams{" + "name=" + name + ", type=" + type + ", xDimensionKey=" + xDimensionKey + ", xDimensionUnit=" + xDimensionUnit + ", xNumPoints=" + xNumPoints + ", metrics=" + metrics + ", dimensions=" + dimensions + ", filters=" + filters + '}';
    }

    @Override
    public Object getMeta()
    {
      HashMap<Object, Object> schema = new HashMap<Object, Object>();
      schema.put(xDimensionKey, metrics);

      HashMap<Object, Object> meta = new HashMap<Object, Object>();
      meta.put(X_KEY, xDimensionKey);
      meta.put(X_UNIT, xDimensionUnit);
      meta.put(FILTERS, filters);
      meta.put(NAME, name);
      meta.put(TYPE, type);
      meta.put(Y_KEYS, metrics);
      meta.put(SCHEMA, schema);

      return meta;
    }

  }

}
