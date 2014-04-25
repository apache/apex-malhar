/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.etl;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAGContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.apps.etl.MapAggregator.MapAggregateEvent;
import com.datatorrent.common.util.Pair;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.python.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class XYLineChartFromMapAggregateOperator extends XYLineChartOperator<MapAggregateEvent>
{
  long currentTimeStamp;
  @Nonnull
  private MapAggregator[] aggregators;
  private List<Set<String>> dimensions;
  private Map<Object, Map<Object, Object>> out = new HashMap<Object, Map<Object, Object>>();
  final public transient DefaultOutputPort<Map<Object, Map<Object, Object>>> outputPort = new DefaultOutputPort<Map<Object, Map<Object, Object>>>();
  private List<LineChartParams> chartParamsList = new ArrayList<LineChartParams>();

  @Override
  protected void processTuple(MapAggregateEvent tuple)
  {
    int aggregatorIndex = tuple.getAggregatorIndex();
    Set<String> dimensionKeys = dimensions.get(aggregatorIndex);
    TimeUnit bucket = TimeUnit.valueOf(tuple.getDimension("bucket").toString());
    long time = (Long)tuple.getDimension(Constants.TIME_ATTR);

    for (LineChartParams lineChartParams : chartParamsList) {
      if (dimensionKeys.equals(lineChartParams.dimensions)) {
        long duration = TimeUnit.MILLISECONDS.convert(lineChartParams.numPoints, bucket);
        //logger.info("time = {} windowtimestamp = {} duration = {} actual diff = {} tuple = {}", time, currentTimeStamp, duration, (currentTimeStamp - time) ,tuple);
        if ((currentTimeStamp - time) > duration) {
          // tuple older than specified by user
          continue;
        }

        boolean isFilterTuple = true;
        for (Entry<String, String> entry : lineChartParams.filters.entrySet()) {
          String key = entry.getKey();
          String val = entry.getValue();
          if (!((String)tuple.getDimension(key)).equalsIgnoreCase(val)) {
            isFilterTuple = false;
            continue;
          }
        }
        if (!isFilterTuple) {
          continue;
        }

        int schemaId = chartParamsList.indexOf(lineChartParams);
        HashMap<Object, Object> xy = (HashMap<Object, Object>)out.get(schemaId);

        if (xy == null) {
          xy = new HashMap<Object, Object>();
          out.put(schemaId, xy);
        }

        ArrayList<Object> yList = new ArrayList<Object>();
        yList.add(tuple.getMetric(Constants.COUNT_DEST));
        yList.add(tuple.getMetric(Constants.BYTES_SUM_DEST));

        xy.put(tuple.getDimension(lineChartParams.xDimensionKey), yList);
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

  public void SetLineChartParams(LineChartParams[] params)
  {
    chartParamsList = Lists.newArrayList(params);
  }

  private static final Logger logger = LoggerFactory.getLogger(XYLineChartFromMapAggregateOperator.class);
}
