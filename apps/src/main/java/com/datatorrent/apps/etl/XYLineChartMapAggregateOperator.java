/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.apps.etl;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.apps.etl.Chart.LineChartParams;

import com.datatorrent.apps.etl.MapAggregator.MapAggregateEvent;

/**
 *
 */
public class XYLineChartMapAggregateOperator extends ChartOperator<MapAggregateEvent, LineChartParams>
{
  private long currentTimeStamp;
  @Nonnull
  private MapAggregator[] aggregators;
  @Nonnull
  private List<Set<String>> dimensions;
  private Map<Object, Object> out = new HashMap<Object, Object>();
  final public transient DefaultOutputPort<Map<Object, Object>> outputPort = new DefaultOutputPort<Map<Object, Object>>();
  private transient boolean isFilterTuple;

  @Override
  protected void processTuple(MapAggregateEvent tuple)
  {
    int aggregatorIndex = tuple.getAggregatorIndex();
    Set<String> dimensionKeys = dimensions.get(aggregatorIndex);

    for (LineChartParams lineChartParams : chartParamsList) {
      if (dimensionKeys.equals(lineChartParams.dimensions)) {
        if (lineChartParams.xDimensionKey.equalsIgnoreCase(Constants.TIME_ATTR)) {
          // time series x axis
          long time = (Long)tuple.getDimension(Constants.TIME_ATTR);
          long duration = TimeUnit.MILLISECONDS.convert(lineChartParams.xNumPoints, TimeUnit.valueOf(lineChartParams.xDimensionUnit));
          if (currentTimeStamp - time > duration) {
            continue;
          }
        }
        else {
          // number series x axis
          double xMax = lineChartParams.xNumPoints * Double.parseDouble(lineChartParams.xDimensionUnit);
          double dimension = (Double)tuple.getDimension(lineChartParams.xDimensionKey);
          if (dimension > xMax) {
            continue;
          }
        }

        isFilterTuple = filterTuple(tuple, lineChartParams);
        if (isFilterTuple) {
          genereteOutput(tuple, lineChartParams);
        }

      }
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentTimeStamp = System.currentTimeMillis();
  }

  @Override
  public void endWindow()
  {
    if (!out.isEmpty()) {
      outputPort.emit(out);
      out.clear();
    }
  }

  public MapAggregator[] getAggregators()
  {
    return aggregators;
  }

  public void setAggregators(MapAggregator[] aggregators)
  {
    this.aggregators = aggregators;
    dimensions = new ArrayList<Set<String>>();
    for (MapAggregator mapAggregator : aggregators) {
      Set<String> dimension = new TreeSet<String>(mapAggregator.getDimensionKeys());
      dimensions.add(dimension);
    }
  }

  private boolean filterTuple(MapAggregateEvent tuple, LineChartParams lineChartParams)
  {
    for (Entry<String, String> entry : lineChartParams.filters.entrySet()) {
      String key = entry.getKey();
      String val = entry.getValue();
      if (!((String)tuple.getDimension(key)).equalsIgnoreCase(val)) {
        return false;
      }
    }
    return true;
  }

  private void genereteOutput(MapAggregateEvent tuple, LineChartParams lineChartParams)
  {
    int schemaId = chartParamsList.indexOf(lineChartParams);
    HashMap<Object, Object> xy = (HashMap<Object, Object>)out.get(schemaId);

    if (xy == null) {
      xy = new HashMap<Object, Object>();
      out.put(schemaId, xy);
    }

    ArrayList<Object> yList = new ArrayList<Object>();
    for (String metric : lineChartParams.metrics) {
      yList.add(tuple.getMetric(metric));
    }

    xy.put(tuple.getDimension(lineChartParams.xDimensionKey), yList);

  }

}
