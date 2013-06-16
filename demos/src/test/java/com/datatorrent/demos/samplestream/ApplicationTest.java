/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.samplestream;

import org.junit.Test;

import com.datatorrent.demos.samplestream.Application;
import com.malhartech.api.LocalMode;

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
    LocalMode.runApp(new Application(), 10000);
  }
}
