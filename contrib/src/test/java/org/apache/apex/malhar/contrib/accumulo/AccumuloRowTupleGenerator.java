/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.contrib.accumulo;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

/**
 *
 */
public class AccumuloRowTupleGenerator extends BaseOperator implements InputOperator
{

  int rowCount;

  public final transient DefaultOutputPort<AccumuloTuple> outputPort = new DefaultOutputPort<AccumuloTuple>();

  @Override
  public void emitTuples()
  {
    AccumuloTuple tuple = new AccumuloTuple();
    tuple.setRow("row" + rowCount);
    tuple.setColFamily("colfam0");
    tuple.setColName("col" + "-" + 0);
    tuple.setColValue("val" + "-" + rowCount + "-" + 0);
    ++rowCount;
    if (rowCount == 99999) {
      rowCount = 0;
    }
    outputPort.emit(tuple);
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    rowCount = 0;
  }

}
