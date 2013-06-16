/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.pi;

import com.datatorrent.api.LocalMode;
import com.datatorrent.demos.pi.Calculator;
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
