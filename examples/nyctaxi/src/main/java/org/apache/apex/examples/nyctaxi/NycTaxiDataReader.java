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
package org.apache.apex.examples.nyctaxi;

import java.io.IOException;

import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;

/**
 * Operator that reads historical New York City Yellow Cab ride data
 * from http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml.
 *
 * Note that unlike the raw LineByLineFileInputOperator, we advance the streaming
 * window whenever we see a difference in the timestamp in the data.
 *
 * @since 3.8.0
 */
public class NycTaxiDataReader extends LineByLineFileInputOperator
{
  private String currentTimestamp;
  private transient boolean suspendEmit = false;

  public NycTaxiDataReader()
  {
    // Whether or not to advance the window does not depend on the size. It solely
    // depends on the timestamp of the data. This is why we are setting this to Integer.MAX_VALUE.
    // See below for "suspendEmit".
    emitBatchSize = Integer.MAX_VALUE;
  }

  @Override
  protected boolean suspendEmit()
  {
    return suspendEmit;
  }

  @Override
  protected String readEntity() throws IOException
  {
    String line = super.readEntity();
    String[] fields = line.split(",", -1);
    if (fields.length > 1) {
      String timestamp = fields[1];
      if (currentTimestamp == null) {
        currentTimestamp = timestamp;
      } else if (timestamp != currentTimestamp) {
        // suspend emit until the next streaming window when timestamp is different from the current timestamp.
        suspendEmit = true;
        currentTimestamp = timestamp;
      }
    }
    return line;
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    // Resume emit since we now have a new streaming window.
    suspendEmit = false;
  }
}
