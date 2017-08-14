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
package org.apache.apex.malhar.lib.util.time;

import java.math.BigDecimal;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

public class WindowUtilsTest
{
  @Test
  public void getAppWindowDurationMSTest()
  {
    OperatorContext context = createOperatorContext(500, 10);
    long appWindowDuration = WindowUtils.getAppWindowDurationMs(context);
    Assert.assertEquals(5000L, appWindowDuration);
  }

  @Test
  public void getAppWindowDurationMSOverflowTest()
  {
    OperatorContext context = createOperatorContext(Integer.MAX_VALUE, Integer.MAX_VALUE);
    long appWindowDuration = WindowUtils.getAppWindowDurationMs(context);

    Assert.assertEquals(new BigDecimal(Integer.MAX_VALUE).multiply(new BigDecimal(Integer.MAX_VALUE)),
        new BigDecimal(appWindowDuration));
  }

  @Test
  public void msToAppWindowCountSimpleTest()
  {
    OperatorContext context = createOperatorContext(500, 10);
    long appWindowCount = WindowUtils.msToAppWindowCount(context, 10000L);
    Assert.assertEquals(2, appWindowCount);

    appWindowCount = WindowUtils.msToAppWindowCount(context, 10001L);
    Assert.assertEquals(3, appWindowCount);
  }

  @Test
  public void msToAppWindowCountRoundingTest()
  {
    OperatorContext context = createOperatorContext(500, 10);
    long appWindowCount = WindowUtils.msToAppWindowCount(context, 10001L);
    Assert.assertEquals(3, appWindowCount);
  }

  @Test
  public void tpsToTpwSimpleTest()
  {
    OperatorContext context = createOperatorContext(100, 1);

    long tuplesPerWindow = WindowUtils.tpsToTpw(context, 500L);
    Assert.assertEquals(50L, tuplesPerWindow);

  }

  @Test
  public void tpsToTpwRoundingTest()
  {
    OperatorContext context = createOperatorContext(100, 1);

    long tuplesPerWindow = WindowUtils.tpsToTpw(context, 501L);
    Assert.assertEquals(51L, tuplesPerWindow);
  }

  @Test
  public void tpsToTpwOverflowTest()
  {
    OperatorContext context = createOperatorContext(Integer.MAX_VALUE, 1);

    boolean overflowException = false;

    try {
      WindowUtils.tpsToTpw(context, Long.MAX_VALUE);
    } catch (Exception e) {
      overflowException = true;
    }

    Assert.assertTrue(overflowException);
  }

  public static OperatorContext createOperatorContext(int streamingWindowMillis, int appWindowCount)
  {
    DefaultAttributeMap attributeMap = new DefaultAttributeMap();
    attributeMap.put(DAGContext.STREAMING_WINDOW_SIZE_MILLIS, streamingWindowMillis);
    attributeMap.put(OperatorContext.APPLICATION_WINDOW_COUNT, appWindowCount);

    return mockOperatorContext(1, attributeMap);
  }
}
