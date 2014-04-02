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
 */package com.datatorrent.apps.logstream;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.lib.logs.DimensionObject;
import java.util.HashMap;
import java.util.Map;

/**
 * Log Score
 */
public class LogScoreOperator extends BaseOperator
{
  public final transient DefaultInputPort<Map<String, DimensionObject<String>>> apacheLogs = new DefaultInputPort<Map<String, DimensionObject<String>>>()
  {

    @Override
    public void process(Map<String, DimensionObject<String>> t)
    {
      // TODO
    }

  };

  public final transient DefaultInputPort<Map<String, DimensionObject<String>>> mysqlLogs = new DefaultInputPort<Map<String, DimensionObject<String>>>()
  {

    @Override
    public void process(Map<String, DimensionObject<String>> t)
    {
      // TODO
    }

  };

  public final transient DefaultInputPort<Map<String, DimensionObject<String>>> syslogLogs = new DefaultInputPort<Map<String, DimensionObject<String>>>()
  {

    @Override
    public void process(Map<String, DimensionObject<String>> t)
    {
      // TODO
    }

  };

  public final transient DefaultInputPort<HashMap<String, Object>> systemLogs = new DefaultInputPort<HashMap<String, Object>>()
  {

    @Override
    public void process(HashMap<String, Object> t)
    {
      // TODO
    }

  };

}
