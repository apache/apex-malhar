/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.twitter;

import com.datatorrent.api.LocalMode;
import com.datatorrent.demos.twitter.TwitterSampleInput;
import com.datatorrent.demos.twitter.TwitterTopCounterApplication;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest
{
  /**
   * This test requires twitter authentication setup and is skipped by default
   * (see {@link TwitterSampleInput}).
   *
   * @throws Exception
   */
  @Test
  public void testApplication() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    new TwitterTopCounterApplication().populateDAG(lma.getDAG(), new Configuration(false));
    LocalMode.Controller lc = lma.getController();
    lc.run(120000);
  }
}
