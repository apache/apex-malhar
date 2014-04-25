/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.etl;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.lib.chart.ChartOperator;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public abstract class XYLineChartOperator<T> extends ChartOperator
{
  List<LineChartParams> charts = new ArrayList<LineChartParams>();
  @InputPortFieldAnnotation(name = "in", optional = true)
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T t)
    {
      processTuple(t);
    }

  };

  protected abstract void processTuple(T t);

  public List<LineChartParams> getCharts()
  {
    return charts;
  }

  public void setCharts(List<LineChartParams> charts)
  {
    this.charts = charts;
  }

  @Override
  public Type getChartType()
  {
    return Type.LINE;
  }

  public static class LineChartParams
  {
    //timestamp=MINUTE:country=usa
    String name;
    String xDimensionKey;
    String yName;
    int numPoints;
    String dimension;
    String metric;
    Set<String> dimensions;
    Map<String, String> filters;
    TimeUnit unit;

    public LineChartParams()
    {
    }

    public void init(String params)
    {
      dimensions = Sets.newTreeSet();
      filters = Maps.newHashMap();
      String[] attributes = params.split(":");

      for (String attribute : attributes) {
        String[] keyval = attribute.split("=", 2);
        filters.put(keyval[0], keyval[1]);
        String key = keyval[0];
        if (key.equals("bucket")) {
          unit = TimeUnit.valueOf(keyval[1]);
          dimensions.add(Constants.TIME_ATTR);
        }
        dimensions.add(key);
      }
    }

    public String getName()
    {
      return name;
    }

    public void setName(String name)
    {
      this.name = name;
    }

    public String getxDimensionKey()
    {
      return xDimensionKey;
    }

    public void setxDimensionKey(String xDimensionkey)
    {
      this.xDimensionKey = xDimensionkey;
    }

    public String getyName()
    {
      return yName;
    }

    public void setyName(String yName)
    {
      this.yName = yName;
    }

    public int getNumPoints()
    {
      return numPoints;
    }

    public void setNumPoints(int numPoints)
    {
      this.numPoints = numPoints;
    }

    public String getDimension()
    {
      return dimension;
    }

    public void setDimension(String dimension)
    {
      this.dimension = dimension;
    }

    public String getMetric()
    {
      return metric;
    }

    public void setMetric(String metric)
    {
      this.metric = metric;
    }

    @Override
    public String toString()
    {
      return "LineChartParams{" + "name=" + name + ", xName=" + xDimensionKey + ", yName=" + yName + ", numPoints=" + numPoints + ", dimension=" + dimension + ", metric=" + metric + ", dimensions=" + dimensions + ", filters=" + filters + ", unit=" + unit + '}';
    }

  }

}
