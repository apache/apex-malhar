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
package org.apache.apex.benchmark.spillable;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

/**
 * @since 3.6.0
 */
public class SpillableTestInputOperator extends BaseOperator implements InputOperator
{
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();
  public long count = 0;
  public int batchSize = 100;
  public int sleepBetweenBatch = 0;

  @Override
  public void emitTuples()
  {
    for (int i = 0; i < batchSize; ++i) {
      output.emit("" + ++count);
    }
    if (sleepBetweenBatch > 0) {
      try {
        Thread.sleep(sleepBetweenBatch);
      } catch (InterruptedException e) {
        //ignore
      }
    }
  }
}
