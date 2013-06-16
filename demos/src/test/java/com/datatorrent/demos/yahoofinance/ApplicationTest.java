/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.yahoofinance;

import com.datatorrent.demos.yahoofinance.Application;
import com.malhartech.api.LocalMode;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

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
    LocalMode lma = LocalMode.newInstance();
    new Application().populateDAG(lma.getDAG(), new Configuration(false));
    LocalMode.Controller lc = lma.getController();
    lc.run(10000);
  }

}
