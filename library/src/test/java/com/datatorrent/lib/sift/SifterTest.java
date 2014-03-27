/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.sift;

import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.datatorrent.lib.testbench.CollectorTestSink;

/**
 * Tests for {@link Sifter}
 */
public class SifterTest
{
  @Test
  public void testSifter()
  {
    Predicate<Map<String, Object>> testEqualsSiftPred = new Predicate<Map<String, Object>>()
    {
      @Override
      public boolean apply(@Nullable Map<String, Object> input)
      {
        Object testValue = input.get("test");
        return testValue != null && testValue.equals("SifterTest");
      }
    };

    Predicate<Map<String, Object>> packageEqualsSiftPred = new Predicate<Map<String, Object>>()
    {
      @Override
      public boolean apply(@Nullable Map<String, Object> input)
      {
        Object packageValue = input.get("package");
        return packageValue != null && packageValue.equals("sift");
      }
    };
    List<Predicate<Map<String, Object>>> predicates = Lists.newArrayList();
    predicates.add(testEqualsSiftPred);
    predicates.add(packageEqualsSiftPred);

    Map<String, Object> event1 = ImmutableMap.of("test", (Object) "SifterTest", "package", (Object) "notSift");
    Map<String, Object> event2 = ImmutableMap.of("test", (Object) "notSifterTest", "package", (Object) "sift");
    Map<String, Object> event3 = ImmutableMap.of("test", (Object) "notSifterTest", "package", (Object) "notSift");

    Sifter<Map<String, Object>> sifter = new Sifter<Map<String, Object>>();
    sifter.setPredicates(predicates);

    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    sifter.output.setSink(sink);

    sifter.beginWindow(0);
    sifter.input.process(event1);
    sifter.input.process(event2);
    sifter.input.process(event3);
    sifter.endWindow();
    Assert.assertEquals("output tuples", 1, sink.collectedTuples.size());
    sink.clear();


    Map<String, Object> event4 = ImmutableMap.of("test", (Object) 3, "package", (Object) "no package");
    Map<String, Object> event5 = ImmutableMap.of("test", (Object) 4, "package", (Object) 7);
    Map<String, Object> event6 = ImmutableMap.of("test", (Object) 4, "package", (Object) "notSift");
    sifter.beginWindow(1);
    sifter.input.process(event4);
    sifter.input.process(event5);
    sifter.input.process(event6);
    sifter.endWindow();
    Assert.assertEquals("output tuples", 3, sink.collectedTuples.size());
    sink.clear();
  }
}
