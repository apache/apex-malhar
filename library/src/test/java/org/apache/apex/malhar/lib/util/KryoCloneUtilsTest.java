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
package org.apache.apex.malhar.lib.util;

import java.util.Objects;

import org.junit.Test;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.lang3.RandomStringUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Unit test for KryoCloneUtils
 */
public class KryoCloneUtilsTest
{

  @Test
  public void testGetClone() throws Exception
  {
    TestEntity from = getTestEntity(5);
    KryoCloneUtils<TestEntity> cloneUtils = KryoCloneUtils.createCloneUtils(from);
    TestEntity to = cloneUtils.getClone();
    assertFalse(from == to);
    assertEquals(from, to);
    assertFalse(from.transientProp.equals(to.transientProp));
  }

  @Test
  public void testGetClones() throws Exception
  {
    TestEntity from = getTestEntity(5);
    KryoCloneUtils<TestEntity> cloneUtils = KryoCloneUtils.createCloneUtils(from);
    TestEntity[] to = cloneUtils.getClones(10);
    for (TestEntity te : to) {
      assertFalse(te == from);
      assertEquals(from, te);
      assertFalse(from.transientProp.equals(te.transientProp));
    }
  }

  @Test
  public void testCloneObject() throws Exception
  {
    TestEntity from = getTestEntity(5);
    TestEntity to = KryoCloneUtils.cloneObject(from);
    assertFalse(from == to);
    assertEquals(from, to);
    assertFalse(from.transientProp.equals(to.transientProp));
  }

  private TestEntity getTestEntity(int depth)
  {
    TestEntity returnVal = null;
    TestEntity curr = null;
    while (depth-- > 0) {
      if (curr == null) {
        curr = returnVal = new TestEntity();
      } else {
        curr.nestedProp = new TestEntity();
        curr = curr.nestedProp;
      }
    }
    return returnVal;
  }

  static class TestEntity
  {

    String strProp = RandomStringUtils.random(10);

    int intProp = RandomUtils.nextInt(1000);

    // transient should be skipped
    transient String transientProp = RandomStringUtils.random(20);

    // deep clone should be supported
    TestEntity nestedProp = null;

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TestEntity that = (TestEntity)o;
      return Objects.equals(intProp, that.intProp) &&
        Objects.equals(strProp, that.strProp) &&
        Objects.equals(nestedProp, that.nestedProp);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(strProp, intProp, nestedProp);
    }

    @Override
    public String toString()
    {
      return "TestEntity{" +
        "strProp='" + strProp + '\'' +
        ", intProp=" + intProp +
        ", transientProp='" + transientProp + '\'' +
        ", nestedProp=" + nestedProp +
        '}';
    }
  }

}
