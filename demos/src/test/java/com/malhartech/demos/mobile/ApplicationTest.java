/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.mobile;

import com.malhartech.dag.DAG;
import com.malhartech.dag.DAG.Operator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class ApplicationTest
{
  public ApplicationTest()
  {
  }


  /**
   * Test of setUnitTestMode method, of class Application.
   */
  @Test
  public void testSetUnitTestMode()
  {
    ;

  }

  /**
   * Test of setLocalMode method, of class Application.
   */
  @Test
  public void testSetLocalMode()
  {
    System.out.println("setLocalMode");
    Application instance = new Application();
    instance.setLocalMode();
  }

  /**
   * Test of getApplication method, of class Application.
   */
  @Test
  public void testGetApplication()
  {
    System.out.println("getApplication");
    Application instance = new Application();

  }
}
