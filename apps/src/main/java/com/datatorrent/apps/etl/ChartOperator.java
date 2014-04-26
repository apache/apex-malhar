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

import javax.annotation.Nonnull;

import org.python.google.common.collect.Lists;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;

/**
 *
 * @param <TUPLE>
 * @param <CHART>
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public abstract class ChartOperator<TUPLE, CHART> extends BaseOperator
{
  public enum CHART_TYPE
  {
    LINE, LIST, TOP
  }

  @Nonnull
  protected List<CHART> chartParamsList;
  public final transient DefaultInputPort<TUPLE> input = new DefaultInputPort<TUPLE>()
  {
    @Override
    public void process(TUPLE t)
    {
      processTuple(t);
    }

  };

  protected abstract void processTuple(TUPLE t);

  public void setChartParamsList(CHART[] params)
  {
    chartParamsList = Lists.newArrayList(params);
  }

}
