/**
 * Copyright (c) 2016 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
package com.datatorrent.flume.operator;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class AbstractFlumeInputOperatorTest
{
  public AbstractFlumeInputOperatorTest()
  {
  }

  @Test
  public void testThreadLocal()
  {
    ThreadLocal<Set<Integer>> tl = new ThreadLocal<Set<Integer>>()
    {
      @Override
      protected Set<Integer> initialValue()
      {
        return new HashSet<Integer>();
      }

    };
    Set<Integer> get1 = tl.get();
    get1.add(1);
    assertTrue("Just Added Value", get1.contains(1));

    Set<Integer> get2 = tl.get();
    assertTrue("Previously added value", get2.contains(1));
  }

}
