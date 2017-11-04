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
package org.apache.apex.examples.parser.regexparser;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

/**
 * @since 3.8.0
 */
public class ServerLogGenerator extends BaseOperator implements InputOperator
{
  public transient DefaultOutputPort<byte[]> outputPort = new DefaultOutputPort<byte[]>();
  private int tupleRate = 10;
  private transient int tuplesEmmitedinWindow = 0;

  public int getTupleRate()
  {
    return tupleRate;
  }

  public void setTupleRate(int tupleRate)
  {
    this.tupleRate = tupleRate;
  }

  @Override
  public void emitTuples()
  {
    while (tuplesEmmitedinWindow < tupleRate) {
      String line = "2015-10-01T03:14:49.000-07:00 lvn-d1-dev DevServer[9876]: INFO: [EVENT][SEQ=248717]" +
          " 2015:10:01:03:14:49 101 sign-in_id=11111@psop.com ip_address=1.1.1.1  service_id=IP1234-NPB12345_00 " +
          "result=RESULT_SUCCESconsole_id=0000000138e91b4e58236bf32besdafasdfasdfasdfsadf  account_id=11111  platform=pik";
      outputPort.emit(line.getBytes());
      tuplesEmmitedinWindow++;
    }
    if (tuplesEmmitedinWindow == tupleRate) {
      String errorLine = "This is error line that will be emitted on the output port";
      outputPort.emit(errorLine.getBytes());
      tuplesEmmitedinWindow++;
    }
  }

  @Override
  public void endWindow()
  {
    tuplesEmmitedinWindow = 0;
    super.endWindow();
  }
}
