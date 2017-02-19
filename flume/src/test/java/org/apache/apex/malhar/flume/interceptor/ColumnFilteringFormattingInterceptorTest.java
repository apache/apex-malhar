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
package org.apache.apex.malhar.flume.interceptor;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.flume.Context;
import org.apache.flume.interceptor.Interceptor;

import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for {@link ColumnFilteringFormattingInterceptor}
 */
public class ColumnFilteringFormattingInterceptorTest
{
  private static InterceptorTestHelper helper;

  @BeforeClass
  public static void startUp()
  {
    HashMap<String, String> contextMap = new HashMap<String, String>();
    contextMap.put(ColumnFilteringInterceptor.Constants.SRC_SEPARATOR, Byte.toString((byte)2));
    contextMap.put(ColumnFilteringFormattingInterceptor.Constants.COLUMNS_FORMATTER, "{1}\001{2}\001{3}\001");

    helper = new InterceptorTestHelper(new ColumnFilteringFormattingInterceptor.Builder(), contextMap);
  }

  @Test
  public void testInterceptEvent()
  {
    helper.testIntercept_Event();
  }

  @Test
  public void testFiles() throws IOException, URISyntaxException
  {
    helper.testFiles();
  }

  @Test
  public void testInterceptEventWithPrefix()
  {
    HashMap<String, String> contextMap = new HashMap<String, String>();
    contextMap.put(ColumnFilteringInterceptor.Constants.SRC_SEPARATOR, Byte.toString((byte)2));
    contextMap.put(ColumnFilteringFormattingInterceptor.Constants.COLUMNS_FORMATTER, "\001{1}\001{2}\001{3}\001");

    ColumnFilteringFormattingInterceptor.Builder builder = new ColumnFilteringFormattingInterceptor.Builder();
    builder.configure(new Context(contextMap));
    Interceptor interceptor = builder.build();

    assertArrayEquals("Six Fields",
        "\001\001Second\001\001".getBytes(),
        interceptor.intercept(
        new InterceptorTestHelper.MyEvent("First\002\002Second\002\002\002".getBytes())).getBody());
  }

  @Test
  public void testInterceptEventWithLongSeparator()
  {
    HashMap<String, String> contextMap = new HashMap<String, String>();
    contextMap.put(ColumnFilteringInterceptor.Constants.SRC_SEPARATOR, Byte.toString((byte)2));
    contextMap.put(ColumnFilteringFormattingInterceptor.Constants.COLUMNS_FORMATTER, "a{1}bc{2}def{3}ghi");

    ColumnFilteringFormattingInterceptor.Builder builder = new ColumnFilteringFormattingInterceptor.Builder();
    builder.configure(new Context(contextMap));
    Interceptor interceptor = builder.build();
    byte[] body = interceptor.intercept(
        new InterceptorTestHelper.MyEvent("First\002\002Second\002\002\002".getBytes())).getBody();

    assertArrayEquals("Six Fields, " + new String(body), "abcSeconddefghi".getBytes(), body);
  }

  @Test
  public void testInterceptEventWithTerminatingSeparator()
  {
    HashMap<String, String> contextMap = new HashMap<String, String>();
    contextMap.put(ColumnFilteringInterceptor.Constants.SRC_SEPARATOR, Byte.toString((byte)2));
    contextMap.put(ColumnFilteringFormattingInterceptor.Constants.COLUMNS_FORMATTER, "a{1}bc{2}def{3}");

    ColumnFilteringFormattingInterceptor.Builder builder = new ColumnFilteringFormattingInterceptor.Builder();
    builder.configure(new Context(contextMap));
    Interceptor interceptor = builder.build();
    byte[] body = interceptor.intercept(
        new InterceptorTestHelper.MyEvent("First\002\002Second\002\002\002".getBytes())).getBody();

    assertArrayEquals("Six Fields, " + new String(body), "abcSeconddef".getBytes(), body);
  }

  @Test
  public void testInterceptEventWithColumnZero()
  {
    HashMap<String, String> contextMap = new HashMap<String, String>();
    contextMap.put(ColumnFilteringInterceptor.Constants.SRC_SEPARATOR, Byte.toString((byte)2));
    contextMap.put(ColumnFilteringFormattingInterceptor.Constants.COLUMNS_FORMATTER, "{0}\001");

    ColumnFilteringFormattingInterceptor.Builder builder = new ColumnFilteringFormattingInterceptor.Builder();
    builder.configure(new Context(contextMap));
    Interceptor interceptor = builder.build();

    assertArrayEquals("Empty Bytes",
        "\001".getBytes(),
        interceptor.intercept(new InterceptorTestHelper.MyEvent("".getBytes())).getBody());

    assertArrayEquals("One Field",
        "First\001".getBytes(),
        interceptor.intercept(new InterceptorTestHelper.MyEvent("First".getBytes())).getBody());

    assertArrayEquals("Two Fields",
        "\001".getBytes(),
        interceptor.intercept(new InterceptorTestHelper.MyEvent("\002First".getBytes())).getBody());
  }
}
