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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test for Sort accumulation
 */
public class SortTest
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

    @Override
    public boolean equals(Object obj)
    {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      TestPojo1 other = (TestPojo1)obj;
      if (uId != other.uId) {
        return false;
      }
      if (uName == null) {
        if (other.uName != null) {
          return false;
        }
      } else if (!uName.equals(other.uName)) {
        return false;
      }
      return true;
    }
  }

  @Test
  public void SortTestAscDesc()
  {
    Comparator<TestPojo1> comparator = new Comparator<TestPojo1>()
    {

      @Override
      public int compare(TestPojo1 o1, TestPojo1 o2)
      {
        if (o1 == null && o2 == null) {
          return 0;
        } else if (o1 == null) {
          return -1;
        } else if (o2 == null) {
          return 1;
        } else if (o1.getUId() != o2.getUId()) {
          return o1.getUId() - o2.getUId();
        } else {
          return o1.getUName().compareTo(o2.getUName());
        }
      }
    };
    Sort<TestPojo1> sort = new Sort<>(false, comparator);
    TestPojo1 o1 = new TestPojo1(5, "user1");
    TestPojo1 o2 = new TestPojo1(15, "user32");
    TestPojo1 o3 = new TestPojo1(5, "user11");
    TestPojo1 o4 = new TestPojo1(2, "user12");
    TestPojo1 o5 = new TestPojo1(15, "user32");
    List<TestPojo1> ascList = new ArrayList<>();
    ascList.add(o4);
    ascList.add(o1);
    ascList.add(o3);
    ascList.add(o2);
    ascList.add(o5);
    List<TestPojo1> accumulatedValue = sort.defaultAccumulatedValue();
    accumulatedValue = sort.accumulate(accumulatedValue, o1);
    accumulatedValue = sort.accumulate(accumulatedValue, o2);
    accumulatedValue = sort.accumulate(accumulatedValue, o3);
    accumulatedValue = sort.accumulate(accumulatedValue, o4);
    accumulatedValue = sort.accumulate(accumulatedValue, o5);

    Iterator<TestPojo1> it = accumulatedValue.iterator();
    int i = 0;
    while (it.hasNext()) {
      Assert.assertEquals(ascList.get(i), it.next());
      i++;
    }

    sort = new Sort<>(true, comparator);
    List<TestPojo1> descAccumulatedValue = sort.defaultAccumulatedValue();
    descAccumulatedValue = sort.accumulate(descAccumulatedValue, o1);
    descAccumulatedValue = sort.accumulate(descAccumulatedValue, o2);
    descAccumulatedValue = sort.accumulate(descAccumulatedValue, o3);
    descAccumulatedValue = sort.accumulate(descAccumulatedValue, o4);
    descAccumulatedValue = sort.accumulate(descAccumulatedValue, o5);

    it = descAccumulatedValue.iterator();
    i = ascList.size() - 1;
    while (it.hasNext()) {
      Assert.assertEquals(ascList.get(i), it.next());
      i--;
    }
  }
}
