/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.apex.malhar.lib.projection;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;

/**
 * Tests for ProjectionOperator
 */
public class ProjectionTest
{
  private static final Logger logger = LoggerFactory.getLogger(ProjectionTest.class);

  public static class DummyPOJO
  {
    private long projected;
    private long remainder;

    public long getProjected()
    {
      return projected;
    }

    public void setProjected(long projected)
    {
      this.projected = projected;
    }

    public long getRemainder()
    {
      return remainder;
    }

    public void setRemainder(long remainder)
    {
      this.remainder = remainder;
    }
  }

  public static class ProjectedPOJO
  {
    private long projected;

    public long getProjected()
    {
      return projected;
    }

    public void setProjected(long projected)
    {
      this.projected = projected;
    }
  }

  public static class RemainderPOJO
  {
    private long remainder;

    public long getRemainder()
    {
      return remainder;
    }

    public void setRemainder(long remainder)
    {
      this.remainder = remainder;
    }
  }

  private static ProjectionOperator projection;
  private static DummyPOJO data;

  public Long getFieldValue(Object p, String field)
  {
    Long value = 0L;

    for (Field f: p.getClass().getDeclaredFields()) {
      f.setAccessible(true);
      try {
        logger.debug("{} field: {} type: {} val: {}", field, f.getName(), f.getType(), f.get(p));
      } catch (IllegalAccessException e) {
        logger.info("could not access value of field: {} type: {}", f.getName(), f.getType());
      }
    }

    try {
      value = (Long)p.getClass().getDeclaredField(field).get(p);
    } catch (NoSuchFieldException e) {
      Assert.assertTrue(e instanceof NoSuchFieldException);
    } catch (IllegalAccessException e) {
      Assert.assertTrue(e instanceof IllegalAccessException);
    }

    return value;
  }

  public void checkProjected(Object p, Integer val)
  {
    Long value = ((ProjectedPOJO)p).getProjected();

    Assert.assertEquals("projected field value", new Long(val), value);
  }

  public void checkRemainder(Object r, Integer val)
  {
    Long value = ((RemainderPOJO)r).getRemainder();

    Assert.assertEquals("remainder field value", new Long(val), value);
  }

  @Test
  public void testProjectionRemainder()
  {
    logger.debug("start round 0");
    projection.beginWindow(0);

    data.setProjected(1234);
    data.setRemainder(6789);

    Object p = null;
    try {
      p = projection.getProjectedObject(data);
    } catch (IllegalAccessException e) {
      Assert.assertTrue(e instanceof IllegalAccessException);
    }
    logger.debug("projected class {}", p.getClass());

    Object r = null;
    try {
      r = projection.getRemainderObject(data);
    } catch (IllegalAccessException e) {
      Assert.assertTrue(e instanceof IllegalAccessException);
    }
    logger.debug("remainder class {}", r.getClass());

    checkProjected(p, 1234);
    checkRemainder(r, 6789);

    projection.endWindow();
    logger.debug("end round 0");
  }

  @Test
  public void testProjected()
  {
    logger.debug("start round 0");
    projection.beginWindow(0);

    data.setProjected(2345);
    data.setRemainder(5678);

    Object p = null;
    try {
      p = projection.getProjectedObject(data);
    } catch (IllegalAccessException e) {
      Assert.assertTrue(e instanceof IllegalAccessException);
    }
    logger.debug("projected class {}", p.getClass());

    checkProjected(p, 2345);

    projection.endWindow();
    logger.debug("end round 0");
  }

  @Test
  public void testRemainder()
  {
    logger.debug("start round 0");
    projection.beginWindow(0);

    data.setProjected(9876);
    data.setRemainder(4321);

    Object r = null;
    try {
      r = projection.getRemainderObject(data);
    } catch (IllegalAccessException e) {
      Assert.assertTrue(e instanceof IllegalAccessException);
    }
    logger.debug("remainder class {}", r.getClass());

    checkRemainder(r, 4321);

    projection.endWindow();
    logger.debug("end round 0");
  }

  @Test
  public void testProjection()
  {
    logger.debug("start round 0");
    projection.beginWindow(0);

    projection.input.process(data);
    Assert.assertEquals("projected tuples", 1, projection.projectedTuples);
    Assert.assertEquals("remainder tuples", 0, projection.remainderTuples);

    projection.endWindow();
    logger.debug("end round 0");

    CollectorTestSink projectedSink = new CollectorTestSink();
    CollectorTestSink remainderSink = new CollectorTestSink();

    projection.projected.setSink(projectedSink);
    projection.remainder.setSink(remainderSink);

    /* Collector Sink Test when remainder port is connected */
    logger.debug("start round 1");
    projection.beginWindow(1);

    data.setProjected(4321);
    data.setRemainder(9876);

    projection.input.process(data);
    Assert.assertEquals("projected tuples", 1, projection.projectedTuples);
    Assert.assertEquals("remainder tuples", 1, projection.remainderTuples);

    Object p = projectedSink.collectedTuples.get(0);
    Object r = remainderSink.collectedTuples.get(0);

    checkProjected(p, 4321);
    checkRemainder(r, 9876);

    projection.endWindow();
    logger.debug("end round 1");
  }

  @BeforeClass
  public static void setup()
  {
    data = new DummyPOJO();
    projection = new ProjectionOperator();
    projection.inClazz = DummyPOJO.class;
    projection.projectedClazz = ProjectedPOJO.class;
    projection.remainderClazz = RemainderPOJO.class;

    List<String> sFields = new ArrayList<>();
    sFields.add("projected");

    projection.setSelectFields(sFields);
    projection.activate(null);
  }

  @AfterClass
  public static void teardown()
  {
    projection.deactivate();
    projection.teardown();
  }
}
