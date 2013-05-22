/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.pi;

import com.malhartech.api.LocalMode;
import org.junit.Test;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class PiCompareApplicationTest
{
  @Test
  public void testSomeMethod() throws Exception
  {
    LocalMode.runApp(new Application(), 10000);
  }
}
