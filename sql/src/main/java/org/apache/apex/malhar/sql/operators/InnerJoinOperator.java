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
package org.apache.apex.malhar.sql.operators;

import org.apache.apex.malhar.lib.join.POJOInnerJoinOperator;
import org.apache.apex.malhar.lib.state.managed.TimeExtractor;
import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.Context;

/**
 * This is an extension of {@link POJOInnerJoinOperator} operator which works over a global scope and
 * does not have time bound expiry of join tuples.
 */
@InterfaceStability.Evolving
public class InnerJoinOperator extends POJOInnerJoinOperator
{
  private long time = System.currentTimeMillis();

  @Override
  public void setup(Context.OperatorContext context)
  {
    this.setExpiryTime(1L);
    // Number of buckets is set to 47000 because this is rounded number closer to sqrt of MAXINT. This guarantees
    // even distribution of keys across buckets.
    this.setNoOfBuckets(47000);
    this.setTimeFieldsStr("");
    super.setup(context);
  }

  public long extractTime(Object tuple, boolean isStream1Data)
  {
    /**
     * Return extract time which is always more than time when the operator is started.
     */
    return time + 3600000L;
  }

  @Override
  public TimeExtractor getTimeExtractor(boolean isStream1)
  {
    return new FixedTimeExtractor();
  }

  public class FixedTimeExtractor implements TimeExtractor
  {
    @Override
    public long getTime(Object o)
    {
      //Return extract time which is always more than time when the operator is started.
      return time + 3600000L;
    }
  }
}
