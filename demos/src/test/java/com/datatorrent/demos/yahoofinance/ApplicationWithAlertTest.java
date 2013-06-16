/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.yahoofinance;

import com.datatorrent.demos.yahoofinance.ApplicationWithAlert;
import com.malhartech.api.LocalMode;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class ApplicationWithAlertTest
{
  public ApplicationWithAlertTest()
  {
  }

  @Test
  public void testSomeMethod() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    new ApplicationWithAlert().populateDAG(lma.getDAG(), new Configuration(false));
    LocalMode.Controller lc = lma.getController();

    long start = System.currentTimeMillis();
    lc.run();
    long end = System.currentTimeMillis();
    long time = end -start;
    System.out.println("Test used "+time+" ms");
  }
}
