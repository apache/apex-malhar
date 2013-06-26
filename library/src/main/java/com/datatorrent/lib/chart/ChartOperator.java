/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.chart;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.PartitionableOperator;

/**
 * This is the base class for all chart operators
 *
 */
public abstract class ChartOperator extends BaseOperator implements PartitionableOperator
{
  /**
   * The different types of chart
   */
  public enum Type
  {
    /**
     * Line - One point for each data item. Both X-axis and Y-axis are numbers. X-axis is usually a time-series
     */
    LINE,
    /**
     * Candle - Four points for each data item (open, close, high, low). Both X-axis and Y-axis are numbers. X-axis is usually a time series
     */
    CANDLE,
    /**
     * Enumerated - X-axis is an enumeration set. Y-axis is a number
     */
    ENUM,
    /**
     * Histogram - Like ENUM, except X-axis is a set of ranges.
     */
    HISTOGRAM,
  }

  /**
   * Gets the chart type
   *
   * @return The chart type
   */
  public abstract Type getChartType();

  @Override
  @SuppressWarnings("unchecked")
  public Collection<Partition<?>> definePartitions(Collection<? extends Partition<?>> partitions, int incrementalCapacity)
  {
    // prevent partitioning
    List<Partition<?>> newPartitions = new ArrayList<Partition<?>>(1);
    newPartitions.add(partitions.iterator().next());
    return newPartitions;
  }

}
