/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.groupby;

import com.malhartech.api.LocalMode;

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
    LocalMode.runApp(new Application(), 5000);
  }
}