/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.wordcount;

import com.datatorrent.api.LocalMode;
import com.datatorrent.demos.wordcount.Application;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class ApplicationTest
{
  public ApplicationTest()
  {
  }

  @Test
  public void testSomeMethod() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    new Application().populateDAG(lma.getDAG(), new Configuration(false));
    LocalMode.Controller lc = lma.getController();

    long start = System.currentTimeMillis();
    lc.run();
    long end = System.currentTimeMillis();
    long time = end -start;
    System.out.println("Test used "+time+" ms");
  }
}
