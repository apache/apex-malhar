/*
 * Copyright (c) 2013 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package com.datatorrent.flume.interceptor;

import java.util.HashMap;

import org.junit.BeforeClass;
import org.junit.Test;

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
    contextMap.put(ColumnFilteringInterceptor.Constants.SRC_SEPARATOR, Byte.toString((byte) 2));
    contextMap.put(ColumnFilteringFormattingInterceptor.Constants.COLUMNS_FORMATTER, "{1}\001{2}\001{3}\001");

    helper = new InterceptorTestHelper(new ColumnFilteringFormattingInterceptor.Builder(), contextMap);
  }

  @Test
  public void testInterceptEvent()
  {
    helper.testIntercept_Event();
  }
}
