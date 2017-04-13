/**
 * Put your copyright and license info here.
 */
package com.example.mydtapp;

import java.io.IOException;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;

/**
 * Test the DAG declaration in local mode.<br>
 * The assumption to run this test case is that test_event_table,meta-table and
 * test_output_event_table are created already
 */
public class ApplicationTest
{

  @Test
  @Ignore
  public void testApplication() throws IOException, Exception
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
      lma.prepareDAG(new JdbcToJdbcApp(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(50000); // runs for 10 seconds and quits
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

}
