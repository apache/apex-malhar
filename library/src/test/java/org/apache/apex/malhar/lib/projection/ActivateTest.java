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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for Projection related Activate method
 */
public class ActivateTest
{
  private static final Logger logger = LoggerFactory.getLogger(ActivateTest.class);

  public static class DummyPOJO
  {
    private long l;
    private String str;
    private Date date;

    public long getL()
    {
      return l;
    }

    public void setL(long l)
    {
      this.l = l;
    }

    public String getStr()
    {
      return str;
    }

    public void setStr(String str)
    {
      this.str = str;
    }

    public Date getDate()
    {
      return date;
    }

    public void setDate(Date date)
    {
      this.date = date;
    }
  }

  private static ProjectionOperator projection;

  @Test
  public void testSelectDropFieldsNull()
  {
    logger.debug("start round 0");
    projection.setSelectFields(null);
    projection.setDropFields(null);
    projection.activate(null);
    Assert.assertEquals("projected fields", 3, projection.getProjectedFields().size());
    Assert.assertEquals("remainder fields", 0, projection.getRemainderFields().size());
    projection.deactivate();
    logger.debug("start round 0");
  }

  @Test
  public void testSelectDropFieldsEmpty()
  {
    logger.debug("start round 0");
    projection.setSelectFields(new ArrayList<String>());
    projection.setDropFields(new ArrayList<String>());
    projection.activate(null);
    Assert.assertEquals("projected fields", 3, projection.getProjectedFields().size());
    Assert.assertEquals("remainder fields", 0, projection.getRemainderFields().size());
    projection.deactivate();
    logger.debug("start round 0");
  }

  @Test
  public void testSelectFields()
  {
    logger.debug("start round 0");
    List<String> sFields = new ArrayList<>();
    sFields.add("l");
    sFields.add("str");
    projection.setSelectFields(sFields);
    projection.setDropFields(new ArrayList<String>());
    projection.activate(null);
    Assert.assertEquals("projected fields", 2, projection.getProjectedFields().size());
    Assert.assertEquals("remainder fields", 1, projection.getRemainderFields().size());
    projection.deactivate();
    logger.debug("start round 0");
  }

  @Test
  public void testDropFields()
  {
    logger.debug("start round 0");
    List<String> dFields = new ArrayList<>();
    dFields.add("str");
    dFields.add("date");
    projection.setDropFields(new ArrayList<String>());
    projection.setDropFields(dFields);
    projection.activate(null);
    Assert.assertEquals("projected fields", 1, projection.getProjectedFields().size());
    Assert.assertEquals("remainder fields", 2, projection.getRemainderFields().size());
    projection.deactivate();
    logger.debug("start round 0");
  }

  @Test
  public void testBothFieldsSpecified()
  {
    logger.debug("start round 0");
    List<String> sFields = new ArrayList<>();
    sFields.add("l");
    sFields.add("str");
    List<String> dFields = new ArrayList<>();
    dFields.add("str");
    dFields.add("date");
    projection.setSelectFields(sFields);
    projection.setDropFields(dFields);
    projection.activate(null);
    Assert.assertEquals("projected fields", 2, projection.getProjectedFields().size());
    Assert.assertEquals("remainder fields", 1, projection.getRemainderFields().size());
    projection.deactivate();
    logger.debug("start round 0");
  }

  @BeforeClass
  public static void setup()
  {
    projection = new ProjectionOperator();
    projection.inClazz = DummyPOJO.class;
    projection.projectedClazz = DummyPOJO.class;
    projection.remainderClazz = DummyPOJO.class;
  }

  @AfterClass
  public static void teardown()
  {
    projection.teardown();
  }
}
