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

import java.util.Comparator;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for Max accumulation
 */
public class MaxTest
{
  @Test
  public void MaxTest()
  {
    Max<Integer> max = new Max<>();

    Assert.assertEquals((Integer)5, max.accumulate(5, 3));
    Assert.assertEquals((Integer)6, max.accumulate(4, 6));
    Assert.assertEquals((Integer)5, max.merge(5, 2));

    Comparator<Integer> com = new Comparator<Integer>()
    {
      @Override
      public int compare(Integer o1, Integer o2)
      {
        return -(o1.compareTo(o2));
      }
    };

    max.setComparator(com);
    Assert.assertEquals((Integer)3, max.accumulate(5, 3));
    Assert.assertEquals((Integer)4, max.accumulate(4, 6));
    Assert.assertEquals((Integer)2, max.merge(5, 2));
  }
}
