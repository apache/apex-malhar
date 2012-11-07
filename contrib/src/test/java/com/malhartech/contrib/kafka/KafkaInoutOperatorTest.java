/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.kafka;

import com.malhartech.api.Context.OperatorContext;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;

import com.malhartech.contrib.kafka.KafkaInputOperator;

@Ignore
public class KafkaInoutOperatorTest
{
  private static OperatorContext config;
  private static KafkaInputOperator instance;


  public KafkaInoutOperatorTest()
  {
  }

  @BeforeClass
  public static void setUpClass() throws Exception
  {
//    config = new OperatorConfiguration();
//    config.set("zk.connect", "localhost:2181");
//    config.set("zk.connectiontimeout.ms", "1000000");
//    config.set("groupid", "test_group");
//    config.set("topic", "test");

    instance = new KafkaInputOperator();
  }

  @AfterClass
  public static void tearDownClass() throws Exception
  {
    config = null;
    instance = null;
  }

  @Before
  public void setUp()
  {
    instance.setup(config);
  }

  @After
  public void tearDown()
  {
    instance.teardown();
  }

  /**
   * Test of setup method, of class KafkaInputOperator.
   */
  public void testSetup()
  {
    System.out.println("setup");
    KafkaInputOperator instance = new KafkaInputOperator();
//    instance.setup(new OperatorConfiguration());
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of run method, of class KafkaInputOperator.
   */
  public void testRun()
  {
    System.out.println("run");
    KafkaInputOperator instance = new KafkaInputOperator();
    //instance.run();
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of teardown method, of class KafkaInputOperator.
   */
  public void testTeardown()
  {
    System.out.println("teardown");
    KafkaInputOperator instance = new KafkaInputOperator();
    instance.teardown();
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }
}
