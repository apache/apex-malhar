/*
 * Copyright (c) 2015 DataTorrent, Inc.
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
package com.datatorrent.lib.appdata.schemas;

import java.util.Map;

import com.google.common.collect.Maps;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

public class SchemaUtilsTest
{
  @Test
  public void findFirstKeyJSONArraySimple() throws Exception
  {
    final String joString = "{\"a\":[\"b\"]}";
    JSONObject jo = new JSONObject(joString);

    JSONArray ja = SchemaUtils.findFirstKeyJSONArray(jo, "a");

    Assert.assertEquals(1, ja.length());
    Assert.assertEquals("b", ja.get(0));
  }

  @Test
  public void findFirstKeyJSONArrayTestNestJSONObject() throws Exception
  {
    final String joString = "{\"a\":{\"b\":[\"c\"]}}";
    JSONObject jo = new JSONObject(joString);

    JSONArray ja = SchemaUtils.findFirstKeyJSONArray(jo, "b");

    Assert.assertEquals(1, ja.length());
    Assert.assertEquals("c", ja.get(0));
  }

  @Test
  public void findFirstKeyJSONArrayTestNestedJSONArray() throws Exception
  {
    final String joString = "{\"a\":[{\"b\":[\"c\"]}]}";
    JSONObject jo = new JSONObject(joString);

    JSONArray ja = SchemaUtils.findFirstKeyJSONArray(jo, "b");

    Assert.assertEquals(1, ja.length());
    Assert.assertEquals("c", ja.get(0));
  }

  @Test
  public void findFirstKeyJSONObjectSimple() throws Exception
  {
    final String joString = "{\"a\":{\"b\":\"c\"}}";
    JSONObject jo = new JSONObject(joString);

    JSONObject joFind = SchemaUtils.findFirstKeyJSONObject(jo, "a");

    Assert.assertEquals(1, joFind.length());
    Assert.assertEquals("c", joFind.getString("b"));
  }

  @Test
  public void findFirstKeyJSONObjectTestNestJSONObject() throws Exception
  {
    final String joString = "{\"a\":{\"b\":{\"c\":\"d\"}}}";
    JSONObject jo = new JSONObject(joString);

    JSONObject joFind = SchemaUtils.findFirstKeyJSONObject(jo, "b");

    Assert.assertEquals(1, joFind.length());
    Assert.assertEquals("d", joFind.getString("c"));
  }

  @Test
  public void findFirstKeyJSONObjectTestNestedJSONArray() throws Exception
  {
    final String joString = "{\"a\":[{\"b\":{\"c\":\"d\"}}]}";
    JSONObject jo = new JSONObject(joString);

    JSONObject joFind = SchemaUtils.findFirstKeyJSONObject(jo, "b");

    Assert.assertEquals(1, joFind.length());
    Assert.assertEquals("d", joFind.get("c"));
  }

  @Test
  public void createJSONObjectFromMapTest() throws Exception
  {
    Map<String, String> mapVals = Maps.newHashMap();
    mapVals.put("a", "1");
    mapVals.put("b", "2");

    JSONObject jo = SchemaUtils.createJSONObject(mapVals);

    Assert.assertEquals(2, jo.length());
    Assert.assertEquals("1", jo.get("a"));
    Assert.assertEquals("2", jo.get("b"));
  }
}
