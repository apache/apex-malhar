/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.apachelog;

import com.malhartech.contrib.apachelog.Application;
import com.malhartech.api.LocalMode;
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
    Application app = new Application();
    LocalMode lma = LocalMode.newInstance();
    app.populateDAG(lma.getDAG(), new Configuration(false));

    final LocalMode.Controller lc = lma.getController();
    long start = System.currentTimeMillis();
    lc.run();
    long end = System.currentTimeMillis();
    long time = end -start;
    System.out.println("Test used "+time+" ms");
  }
}
