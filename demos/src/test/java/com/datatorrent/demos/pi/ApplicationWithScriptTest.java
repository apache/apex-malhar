/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.pi;

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