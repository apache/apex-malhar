/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.chart;

import org.junit.Test;

import com.datatorrent.api.LocalMode;
import com.datatorrent.demos.chart.YahooFinanceApplication;

/**
 * Run Yahoo Finance application demo.
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class ApplicationTest
{
  /**
   * This will run for ever.
   *
   * @throws Exception
   */
  @Test
  public void testApplication() throws Exception
  {
    LocalMode.runApp(new YahooFinanceApplication(), 60000);
  }

}
