/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.demos.dimensions.generic;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.validation.constraints.Min;
import java.util.Random;

/**
 * Generates sales events data and sends them out as JSON encoded byte arrays.
 * <p>
 * Sales events are JSON string encoded as byte arrays.  All id's are expected to be positive integers, and default to 1.
 * Transaction amounts are double values with two decimal places.  Timestamp is unix epoch in milliseconds.
 * Product categories are not assigned by default.  They are expected to be added by the Enrichment operator, but can be
 * enabled with addProductCategory override.
 *
 * Example Sales Event
 *
 * {
 *    "productId": 1,
 *    "customerId": 12345,
 *    "productCategory": 0,
 *    "channelId": 3,
 *    "amount": 107.99,
 *    "timestamp": 1412897574000
 * }
 *
 * @displayName JSON Sales Event Generator
 * @category Input
 * @tags input, generator, json
 */
public class JsonSalesGenerator implements InputOperator
{
  @Min(1)
  private int maxProductId = 100;
  @Min(1)
  private int maxCustomerId = 1000000;
  @Min(1)
  private int maxChannelId = 5;
  @Min(0)
  private double minAmount = 0.99;
  private double maxAmount = 100.0;

  // Should not be included by default - only used for testing when running without enrichment operator
  private boolean addProductCategory = false;
  @Min(1)
  private int maxProductCategories = 5;

  // Limit number of emitted tuples per window
  @Min(0)
  private long maxTuplesPerWindow = 40000;

  // Maximum amount of deviation below the maximum tuples per window
  @Min(0)
  private int tuplesPerWindowDeviation = 1000;

  // Number of windows to maintain the same deviation before selecting another
  @Min(1)
  private int tuplesPerWindowDeviationCycle = 120;


  /**
   * Outputs sales event in JSON format as a byte array
   */
  @OutputPortFieldAnnotation(name = "jsonBytes")
  public final transient DefaultOutputPort<byte[]> jsonBytes = new DefaultOutputPort<byte[]>();

  private static final ObjectMapper mapper = new ObjectMapper().setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
  private final Random random = new Random();

  private long tuplesCounter = 0;
  private long tuplesPerCurrentWindow = maxTuplesPerWindow;

  @Override
  public void beginWindow(long windowId)
  {
    tuplesCounter = 0;
    if (windowId % tuplesPerWindowDeviationCycle == 0) {
      tuplesPerCurrentWindow = maxTuplesPerWindow - random.nextInt(tuplesPerWindowDeviation);
    }
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    tuplesPerCurrentWindow = maxTuplesPerWindow;
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void emitTuples()
  {
    while (tuplesCounter++ < tuplesPerCurrentWindow) {
      try {

        SalesEvent salesEvent = generateSalesEvent();
        this.jsonBytes.emit(mapper.writeValueAsBytes(salesEvent));

      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  SalesEvent generateSalesEvent() throws Exception {
    SalesEvent salesEvent = new SalesEvent();
    salesEvent.timestamp = System.currentTimeMillis();
    salesEvent.productId = randomId(maxProductId);
    salesEvent.customerId = randomId(maxCustomerId);
    salesEvent.channelId = randomId(maxChannelId);
    salesEvent.amount = randomAmount(minAmount, maxAmount);
    if (addProductCategory) {
      salesEvent.productCategory = 1 + (salesEvent.productId % maxProductCategories);
    }
    return salesEvent;
  }

  private int randomId(int max) {
    // Provide safe default for invalid max
    if (max < 1) return 1;
    return 1 + random.nextInt(max);
  }

  // Generate random double with gaussian distribution between min and max and two decimal places
  private double randomAmount(double min, double max) {
    // Provide safe default for invalid min/max relationships
    if (max <= min) return Math.floor(min * 100) / 100;
    double mean = (min + max)/2.0;
    double deviation = (max - min)/2.0/3.0;
    double rawAmount;
    do {
      rawAmount = random.nextGaussian() * deviation + mean;
    } while (rawAmount < minAmount || rawAmount > maxAmount);
    return Math.floor(rawAmount * 100) / 100;
  }


  public long getMaxTuplesPerWindow() {
    return maxTuplesPerWindow;
  }

  public void setMaxTuplesPerWindow(long maxTuplesPerWindow) {
    this.maxTuplesPerWindow = maxTuplesPerWindow;
  }

  public int getMaxProductId() {
    return maxProductId;
  }

  public void setMaxProductId(int maxProductId) {
    this.maxProductId = maxProductId;
  }

  public int getMaxCustomerId() {
    return maxCustomerId;
  }

  public void setMaxCustomerId(int maxCustomerId) {
    this.maxCustomerId = maxCustomerId;
  }

  public int getMaxChannelId() {
    return maxChannelId;
  }

  public void setMaxChannelId(int maxChannelId) {
    this.maxChannelId = maxChannelId;
  }

  public double getMinAmount() {
    return minAmount;
  }

  public void setMinAmount(double minAmount) {
    this.minAmount = minAmount;
  }

  public double getMaxAmount() {
    return maxAmount;
  }

  public void setMaxAmount(double maxAmount) {
    this.maxAmount = maxAmount;
  }

  public boolean isAddProductCategory() {
    return addProductCategory;
  }

  public void setAddProductCategory(boolean addProductCategory) {
    this.addProductCategory = addProductCategory;
  }

  public int getMaxProductCategories() {
    return maxProductCategories;
  }

  public void setMaxProductCategories(int maxProductCategories) {
    this.maxProductCategories = maxProductCategories;
  }

  public int getTuplesPerWindowDeviation() {
    return tuplesPerWindowDeviation;
  }

  public void setTuplesPerWindowDeviation(int tuplesPerWindowDeviation) {
    this.tuplesPerWindowDeviation = tuplesPerWindowDeviation;
  }
}

class SalesEvent {

  /* dimension keys */
  public int productId;
  public int customerId;
  public int channelId;
  public int productCategory;
  public long timestamp;
  /* metrics */
  public double amount;
}
