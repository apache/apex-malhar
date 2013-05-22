/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.pi;

import com.malhartech.api.LocalMode;
import org.junit.Test;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class CalculatorTest
{
  @Test
  public void testSomeMethod() throws Exception
  {
    LocalMode.runApp(new Calculator(), 10000);
  }
}
