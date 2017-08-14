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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.util.KeyValPair;

import com.google.common.collect.Multimap;

/**
 * Test for POJO outer join accumulations
 */
public class PojoOuterJoinTest
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

    public int getUId()
    {
      return uId;
    }

    public void setUId(int uId)
    {
      this.uId = uId;
    }

    public String getUName()
    {
      return uName;
    }

    public void setUName(String uName)
    {
      this.uName = uName;
    }
  }

  public static class TestPojo3
  {
    private int uId;
    private String uNickName;
    private int age;

    public TestPojo3()
    {

    }

    public TestPojo3(int id, String name, int age)
    {
      this.uId = id;
      this.uNickName = name;
      this.age = age;
    }

    public int getUId()
    {
      return uId;
    }

    public void setUId(int uId)
    {
      this.uId = uId;
    }

    public String getUNickName()
    {
      return uNickName;
    }

    public void setUNickName(String uNickName)
    {
      this.uNickName = uNickName;
    }

    public int getAge()
    {
      return age;
    }

    public void setAge(int age)
    {
      this.age = age;
    }
  }


  public static class TestOutClass
  {
    private int uId;
    private String uName;
    private String uNickName;
    private int age;

    public int getUId()
    {
      return uId;
    }

    public void setUId(int uId)
    {
      this.uId = uId;
    }

    public String getUName()
    {
      return uName;
    }

    public void setUName(String uName)
    {
      this.uName = uName;
    }

    public String getUNickName()
    {
      return uNickName;
    }

    public void setUNickName(String uNickName)
    {
      this.uNickName = uNickName;
    }

    public int getAge()
    {
      return age;
    }

    public void setAge(int age)
    {
      this.age = age;
    }
  }

  public static class TestOutMultipleKeysClass
  {
    private int uId;
    private String uName;
    private int age;

    public int getUId()
    {
      return uId;
    }

    public void setUId(int uId)
    {
      this.uId = uId;
    }

    public String getUName()
    {
      return uName;
    }

    public void setUName(String uName)
    {
      this.uName = uName;
    }

    public int getAge()
    {
      return age;
    }

    public void setAge(int age)
    {
      this.age = age;
    }
  }


  @Test
  public void PojoLeftOuterJoinTest()
  {
    String[] leftKeys = {"uId"};
    String[] rightKeys = {"uId"};
    PojoLeftOuterJoin<TestPojo1, TestPojo3> pij = new PojoLeftOuterJoin<>(TestOutClass.class, leftKeys, rightKeys);
    List<Multimap<List<Object>, Object>> accu = pij.defaultAccumulatedValue();
    Assert.assertEquals(2, accu.size());
    accu = pij.accumulate(accu, new TestPojo1(1, "Josh"));
    accu = pij.accumulate(accu, new TestPojo1(2, "Bob"));
    accu = pij.accumulate2(accu, new TestPojo3(1, "NickJosh", 12));
    accu = pij.accumulate2(accu, new TestPojo3(3, "NickBob", 13));

    List result = pij.getOutput(accu);
    Assert.assertEquals(2, result.size());
    Object o = result.get(0);
    Assert.assertTrue(o instanceof TestOutClass);
    TestOutClass testOutClass = (TestOutClass)o;
    int uId = testOutClass.getUId();
    if (uId == 1) {
      checkNameAge("Josh",12,testOutClass);
      o = result.get(1);
      Assert.assertTrue(o instanceof TestOutClass);
      testOutClass = (TestOutClass)o;
      uId = testOutClass.getUId();
      Assert.assertEquals(2, testOutClass.getUId());
      checkNameAge("Bob",0,testOutClass);
    } else if (uId == 2) {
      checkNameAge("Bob",0,testOutClass);
      o = result.get(1);
      Assert.assertTrue(o instanceof TestOutClass);
      testOutClass = (TestOutClass)o;
      uId = testOutClass.getUId();
      Assert.assertEquals(1, testOutClass.getUId());
      checkNameAge("Josh",12,testOutClass);
    }
  }

  public void checkNameAge(String name, int age, TestOutClass testOutClass)
  {
    Assert.assertEquals(name, testOutClass.getUName());
    Assert.assertEquals(age, testOutClass.getAge());
  }

  @Test
  public void PojoRightOuterJoinTest()
  {
    String[] leftKeys = {"uId"};
    String[] rightKeys = {"uId"};
    PojoRightOuterJoin<TestPojo1, TestPojo3> pij = new PojoRightOuterJoin<>(TestOutClass.class, leftKeys, rightKeys);
    List<Multimap<List<Object>, Object>> accu = pij.defaultAccumulatedValue();
    Assert.assertEquals(2, accu.size());
    accu = pij.accumulate(accu, new TestPojo1(1, "Josh"));
    accu = pij.accumulate(accu, new TestPojo1(2, "Bob"));
    accu = pij.accumulate2(accu, new TestPojo3(1, "NickJosh", 12));
    accu = pij.accumulate2(accu, new TestPojo3(3, "NickBob", 13));

    List result = pij.getOutput(accu);
    Assert.assertEquals(2, result.size());
    Object o = result.get(0);
    Assert.assertTrue(o instanceof TestOutClass);
    TestOutClass testOutClass = (TestOutClass)o;
    int uId = testOutClass.getUId();
    if (uId == 1) {
      checkNameAge("Josh",12,testOutClass);
      o = result.get(1);
      Assert.assertTrue(o instanceof TestOutClass);
      testOutClass = (TestOutClass)o;
      uId = testOutClass.getUId();
      Assert.assertEquals(3, testOutClass.getUId());
      checkNameAge(null,13,testOutClass);
    } else if (uId == 3) {
      checkNameAge(null,13,testOutClass);
      o = result.get(1);
      Assert.assertTrue(o instanceof TestOutClass);
      testOutClass = (TestOutClass)o;
      uId = testOutClass.getUId();
      Assert.assertEquals(1, testOutClass.getUId());
      checkNameAge("Josh",12,testOutClass);
    }
  }

  @Test
  public void PojoFullOuterJoinTest()
  {
    String[] leftKeys = {"uId"};
    String[] rightKeys = {"uId"};
    PojoFullOuterJoin<TestPojo1, TestPojo3> pij = new PojoFullOuterJoin<>(TestOutClass.class, leftKeys, rightKeys);
    List<Multimap<List<Object>, Object>> accu = pij.defaultAccumulatedValue();
    Assert.assertEquals(2, accu.size());
    accu = pij.accumulate(accu, new TestPojo1(1, "Josh"));
    accu = pij.accumulate(accu, new TestPojo1(2, "Bob"));
    accu = pij.accumulate2(accu, new TestPojo3(1, "NickJosh", 12));
    accu = pij.accumulate2(accu, new TestPojo3(3, "NickBob", 13));

    Assert.assertEquals(3, pij.getOutput(accu).size());
    Set<Integer> checkMap = new HashSet<>();
    for ( int i = 0; i < 3; i++ ) {
      Object o = pij.getOutput(accu).get(i);
      Assert.assertTrue(o instanceof TestOutClass);
      TestOutClass testOutClass = (TestOutClass)o;
      int uId = testOutClass.getUId();
      checkMap.add(uId);
    }
    Assert.assertEquals(3,checkMap.size());
  }

  @Test
  public void PojoLeftOuterJoinTestWithMap()
  {
    String[] leftKeys = {"uId", "uName"};
    String[] rightKeys = {"uId", "uNickName"};
    Map<String,KeyValPair<AbstractPojoJoin.STREAM, String>> outputInputMap = new HashMap<>();
    outputInputMap.put("uId",new KeyValPair<>(AbstractPojoJoin.STREAM.LEFT,"uId"));
    outputInputMap.put("age",new KeyValPair<>(AbstractPojoJoin.STREAM.RIGHT,"age"));
    PojoLeftOuterJoin<TestPojo1, TestPojo3> pij = new PojoLeftOuterJoin<>(TestOutClass.class, leftKeys, rightKeys, outputInputMap);

    List<Multimap<List<Object>, Object>> accu = pij.defaultAccumulatedValue();

    Assert.assertEquals(2, accu.size());

    accu = pij.accumulate(accu, new TestPojo1(1, "Josh"));
    accu = pij.accumulate(accu, new TestPojo1(2, "Bob"));

    accu = pij.accumulate2(accu, new TestPojo3(1, "Josh", 12));
    accu = pij.accumulate2(accu, new TestPojo3(3, "ECE", 13));

    List result = pij.getOutput(accu);
    Assert.assertEquals(2, result.size());
    Object o = result.get(0);
    Assert.assertTrue(o instanceof TestOutClass);
    TestOutClass testOutClass = (TestOutClass)o;
    int uId = testOutClass.getUId();
    if (uId == 1) {
      checkNameAge(null,12,testOutClass);
      o = result.get(1);
      Assert.assertTrue(o instanceof TestOutClass);
      testOutClass = (TestOutClass)o;
      uId = testOutClass.getUId();
      Assert.assertEquals(2, uId);
      checkNameAge(null,0,testOutClass);
    } else if (uId == 2) {
      checkNameAge(null,0,testOutClass);
      o = result.get(1);
      Assert.assertTrue(o instanceof TestOutClass);
      testOutClass = (TestOutClass)o;
      uId = testOutClass.getUId();
      Assert.assertEquals(1, uId);
      checkNameAge(null,12,testOutClass);
    }
  }

  @Test
  public void PojoRightOuterJoinTestWithMap()
  {
    String[] leftKeys = {"uId", "uName"};
    String[] rightKeys = {"uId", "uNickName"};
    Map<String,KeyValPair<AbstractPojoJoin.STREAM, String>> outputInputMap = new HashMap<>();
    outputInputMap.put("uId",new KeyValPair<>(AbstractPojoJoin.STREAM.LEFT,"uId"));
    outputInputMap.put("age",new KeyValPair<>(AbstractPojoJoin.STREAM.RIGHT,"age"));
    PojoRightOuterJoin<TestPojo1, TestPojo3> pij = new PojoRightOuterJoin<>(TestOutClass.class, leftKeys, rightKeys, outputInputMap);

    List<Multimap<List<Object>, Object>> accu = pij.defaultAccumulatedValue();

    Assert.assertEquals(2, accu.size());

    accu = pij.accumulate(accu, new TestPojo1(1, "Josh"));
    accu = pij.accumulate(accu, new TestPojo1(2, "Bob"));

    accu = pij.accumulate2(accu, new TestPojo3(1, "Josh", 12));
    accu = pij.accumulate2(accu, new TestPojo3(3, "Bob", 13));

    List result = pij.getOutput(accu);
    Assert.assertEquals(2, result.size());
    Object o = result.get(0);
    Assert.assertTrue(o instanceof TestOutClass);
    TestOutClass testOutClass = (TestOutClass)o;
    int uId = testOutClass.getUId();
    if (uId == 1) {
      checkNameAge(null,12,testOutClass);
      o = result.get(1);
      Assert.assertTrue(o instanceof TestOutClass);
      testOutClass = (TestOutClass)o;
      uId = testOutClass.getUId();
      Assert.assertEquals(0, uId);
      checkNameAge(null,13,testOutClass);
    } else if (uId == 0) {
      checkNameAge(null,13,testOutClass);
      o = result.get(1);
      Assert.assertTrue(o instanceof TestOutClass);
      testOutClass = (TestOutClass)o;
      uId = testOutClass.getUId();
      Assert.assertEquals(1, uId);
      checkNameAge(null,12,testOutClass);
    }
  }

  @Test
  public void PojoFullOuterJoinTestWithMap()
  {
    String[] leftKeys = {"uId", "uName"};
    String[] rightKeys = {"uId", "uNickName"};
    Map<String,KeyValPair<AbstractPojoJoin.STREAM, String>> outputInputMap = new HashMap<>();
    outputInputMap.put("uId",new KeyValPair<>(AbstractPojoJoin.STREAM.LEFT,"uId"));
    outputInputMap.put("age",new KeyValPair<>(AbstractPojoJoin.STREAM.RIGHT,"age"));
    PojoFullOuterJoin<TestPojo1, TestPojo3> pij = new PojoFullOuterJoin<>(TestOutClass.class, leftKeys, rightKeys, outputInputMap);

    List<Multimap<List<Object>, Object>> accu = pij.defaultAccumulatedValue();

    Assert.assertEquals(2, accu.size());

    accu = pij.accumulate(accu, new TestPojo1(1, "Josh"));
    accu = pij.accumulate(accu, new TestPojo1(2, "Bob"));

    accu = pij.accumulate2(accu, new TestPojo3(1, "Josh", 12));
    accu = pij.accumulate2(accu, new TestPojo3(3, "Bob", 13));

    Assert.assertEquals(3, pij.getOutput(accu).size());
    Set<Integer> checkMap = new HashSet<>();
    for ( int i = 0; i < 3; i++ ) {
      Object o = pij.getOutput(accu).get(i);
      Assert.assertTrue(o instanceof TestOutClass);
      TestOutClass testOutClass = (TestOutClass)o;
      int uId = testOutClass.getUId();
      checkMap.add(uId);
    }
    Assert.assertEquals(3,checkMap.size());
  }
}
