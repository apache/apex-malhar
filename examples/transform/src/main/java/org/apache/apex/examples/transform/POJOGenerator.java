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

package org.apache.apex.examples.transform;

import java.util.Calendar;
import java.util.Date;
import java.util.Random;

import javax.validation.constraints.Min;

import org.apache.commons.lang3.RandomStringUtils;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

/**
 * Generates and emits the CustomerEvent
 *
 * @since 3.7.0
 */
public class POJOGenerator implements InputOperator
{
  public static final int MAX_YEAR_DIFF_RANGE = 30;

  @Min(1)
  private int maxCustomerId = 100000;
  @Min(1)
  private int maxNameLength = 10;
  @Min(1)
  private int maxAddressLength = 15;
  private long tuplesCounter = 0;
  private long currentWindowTuplesCounter = 0;

  // Limit number of emitted tuples per window
  @Min(1)
  private long maxTuplesPerWindow = 100;

  @Min(1)
  private long maxTuples = 500;

  private Date currentDate;

  private final Random random = new Random();
  private final RandomStringUtils rRandom = new RandomStringUtils();
  public final transient DefaultOutputPort<Object> output = new DefaultOutputPort<>();

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowTuplesCounter = 0;
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    currentDate = new Date();
  }

  @Override
  public void teardown()
  {

  }

  CustomerEvent generateCustomersEvent() throws Exception
  {
    CustomerEvent customerEvent = new CustomerEvent();
    customerEvent.setCustomerId(randomId(maxCustomerId));
    customerEvent.setFirstName(rRandom.randomAlphabetic(randomId(maxNameLength)));
    customerEvent.setLastName(rRandom.randomAlphabetic(randomId(maxNameLength)));
    Calendar calendar = Calendar.getInstance();
    // Subtract a random year within a given range to set Date of birth. Used in asserts later
    calendar.add(Calendar.YEAR, (-1 * random.nextInt(MAX_YEAR_DIFF_RANGE)));
    customerEvent.setDateOfBirth(calendar.getTime());
    customerEvent.setAddress(rRandom.randomAlphabetic(randomId(maxAddressLength)));
    return customerEvent;
  }

  private int randomId(int max)
  {
    if (max < 1) {
      return 1;
    }
    return 1 + random.nextInt(max);
  }

  @Override
  public void emitTuples()
  {
    while ( ( currentWindowTuplesCounter < maxTuplesPerWindow) && (tuplesCounter < maxTuples) ) {
      try {
        CustomerEvent event = generateCustomersEvent();
        this.output.emit(event);
        tuplesCounter++;
        currentWindowTuplesCounter++;
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  public long getMaxTuples()
  {
    return maxTuples;
  }

  public void setMaxTuples(long maxTuples)
  {
    this.maxTuples = maxTuples;
  }

  public int getMaxAddressLength()
  {
    return maxAddressLength;
  }

  public void setMaxAddressLength(int maxAddressLength)
  {
    this.maxAddressLength = maxAddressLength;
  }

  public int getMaxNameLength()
  {
    return maxNameLength;
  }

  public void setMaxNameLength(int maxNameLength)
  {
    this.maxNameLength = maxNameLength;
  }

  public int getMaxCustomerId()
  {
    return maxCustomerId;
  }

  public void setMaxCustomerId(int maxCustomerId)
  {
    this.maxCustomerId = maxCustomerId;
  }

  public long getMaxTuplesPerWindow()
  {
    return maxTuplesPerWindow;
  }

  public void setMaxTuplesPerWindow(long maxTuplesPerWindow)
  {
    this.maxTuplesPerWindow = maxTuplesPerWindow;
  }
}
