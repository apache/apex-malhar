/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.helper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;


public class CollectorModule<T> extends BaseOperator
{
  public final transient CollectorInputPort<T> inputPort = new CollectorInputPort<T>("collector", this);

  public static class CollectorInputPort<T> extends DefaultInputPort<T>
  {
    public volatile static HashMap<String, List<?>> collections = new HashMap<String, List<?>>();
    ArrayList<T> list;

    final String id;

    public CollectorInputPort(String id, Operator module)
    {
      super();
      this.id = id;
    }

    @Override
    public void process(T tuple)
    {
  //    System.out.print("collector process:"+tuple);
      list.add(tuple);
    }

    @Override
    public void setConnected(boolean flag)
    {
      if (flag) {
        collections.put(id, list = new ArrayList<T>());
      }
    }
  }
}
