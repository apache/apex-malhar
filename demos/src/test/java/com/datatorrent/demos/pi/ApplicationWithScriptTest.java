/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.pi;

import com.datatorrent.demos.pi.ApplicationWithScript;
import com.malhartech.api.LocalMode;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class ApplicationWithScriptTest
{
  //@Test
  public void testSomeMethod() throws Exception
  {
    LocalMode.runApp(new ApplicationWithScript(), 10000);
  }
}