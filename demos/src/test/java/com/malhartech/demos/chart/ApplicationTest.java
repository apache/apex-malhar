/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.chart;

import org.junit.Test;

import com.malhartech.api.LocalMode;

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
