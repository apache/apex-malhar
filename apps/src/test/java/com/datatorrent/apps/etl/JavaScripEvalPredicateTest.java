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
package com.datatorrent.apps.etl;

import java.util.List;
import java.util.Map;

import junit.framework.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class JavaScripEvalPredicateTest
{
  @Test
  public void test()
  {
    JavaScriptEvalPredicate predicate = new JavaScriptEvalPredicate();
    predicate.setExpression("response === \"404\"");
    List<String> expressionKeys = Lists.newArrayList("response");
    predicate.setExpressionKeys(expressionKeys);

    Map<String, Object> event = Maps.newHashMap();
    event.put("response", "404");

    boolean result = predicate.apply(event);
    Assert.assertTrue(result);
  }
}
