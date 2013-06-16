/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.pi;

import com.datatorrent.api.LocalMode;
import com.datatorrent.demos.pi.Application;
import org.junit.Test;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class ApplicationTest
{
  @Test
  public void testSomeMethod() throws Exception
  {
    LocalMode.runApp(new Application(), 10000);
  }
}