/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.couchdb;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Generates Map objects which can be saved in couchdb<br></br>
 *
 * @since 0.3.5
 */
public class CouchTupleGenerator extends BaseOperator implements InputOperator
{

  private transient int count = 1;

  @OutputPortFieldAnnotation(name = "outputPort")
  public final transient DefaultOutputPort<Map<Object, Object>> outputPort = new DefaultOutputPort<Map<Object, Object>>();

  @Override
  public void emitTuples()
  {
    Map<Object, Object> tuple = Maps.newHashMap();
    tuple.put("_id", "TestDatabase");
    tuple.put("name", "CouchDBGenerator");
    tuple.put("type", "Output");
    tuple.put("value", Integer.toString(count++));

    outputPort.emit(tuple);
  }
}
