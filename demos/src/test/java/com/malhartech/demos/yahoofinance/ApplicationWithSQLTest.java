/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.yahoofinance;

import com.malhartech.api.LocalMode;
import com.malhartech.demos.yahoofinance.ApplicationWithSQL;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class ApplicationWithSQLTest
{
  public ApplicationWithSQLTest()
  {
  }

  @Test
  public void testSomeMethod() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    new ApplicationWithSQL().getApplication(lma.getDAG(), new Configuration(false));
    LocalMode.Controller lc = lma.getController();

    long start = System.currentTimeMillis();
    lc.run();
    long end = System.currentTimeMillis();
    long time = end -start;
    System.out.println("Test used "+time+" ms");
  }
}
