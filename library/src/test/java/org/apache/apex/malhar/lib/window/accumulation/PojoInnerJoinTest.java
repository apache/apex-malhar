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
package org.apache.apex.malhar.lib.window.accumulation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link PojoInnerJoin}.
 */
public class PojoInnerJoinTest
{

  public static class TestPojo1
  {
    private int uId;
    private String uName;

    public TestPojo1()
    {

    }

    public TestPojo1(int id, String name)
    {
      this.uId = id;
      this.uName = name;
    }

    public int getuId()
    {
      return uId;
    }

    public void setuId(int uId)
    {
      this.uId = uId;
    }

    public String getuName()
    {
      return uName;
    }

    public void setuName(String uName)
    {
      this.uName = uName;
    }
  }

  public static class TestPojo2
  {
    private int uId;
    private String dep;

    public TestPojo2()
    {

    }

    public TestPojo2(int id, String dep)
    {
      this.uId = id;
      this.dep = dep;
    }

    public int getuId()
    {
      return uId;
    }

    public void setuId(int uId)
    {
      this.uId = uId;
    }

    public String getDep()
    {
      return dep;
    }

    public void setDep(String dep)
    {
      this.dep = dep;
    }
  }


  @Test
  public void PojoInnerJoinTest()
  {
    PojoInnerJoin<TestPojo1, TestPojo2> pij = new PojoInnerJoin<>(2, "uId", "uId");

    List<List<Map<String, Object>>> accu = pij.defaultAccumulatedValue();

    Assert.assertEquals(2, accu.size());

    accu = pij.accumulate(accu, new TestPojo1(1, "Josh"));
    accu = pij.accumulate(accu, new TestPojo1(2, "Bob"));

    accu = pij.accumulate2(accu, new TestPojo2(1, "CS"));
    accu = pij.accumulate2(accu, new TestPojo2(3, "ECE"));

    Map<String, Object> result = new HashMap<>();
    result.put("uId", 1);
    result.put("uName", "Josh");
    result.put("dep", "CS");

    Assert.assertEquals(1, pij.getOutput(accu).size());
    Assert.assertEquals(result, pij.getOutput(accu).get(0));
  }

}
