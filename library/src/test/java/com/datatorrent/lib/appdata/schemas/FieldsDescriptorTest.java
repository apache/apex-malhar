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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Test;

public class FieldsDescriptorTest
{
  @Test
  public void simpleTest()
  {
    Map<String, Type> fieldToType = Maps.newHashMap();
    final String boolField = "boolField";
    final String charField = "charField";
    final String stringField = "stringField";
    final String objectField = "objectField";
    final String byteField = "byteField";
    final String shortField = "shortField";
    final String integerField = "integerField";
    final String longField = "longField";
    final String floatField = "floatField";
    final String doubleField = "doubleField";

    fieldToType.put(boolField, Type.BOOLEAN);
    fieldToType.put(charField, Type.CHAR);
    fieldToType.put(stringField, Type.STRING);
    fieldToType.put(objectField, Type.OBJECT);
    fieldToType.put(byteField, Type.BYTE);
    fieldToType.put(shortField, Type.SHORT);
    fieldToType.put(integerField, Type.INTEGER);
    fieldToType.put(longField, Type.LONG);
    fieldToType.put(floatField, Type.FLOAT);
    fieldToType.put(doubleField, Type.DOUBLE);

    final List<String> expectedFieldList = Lists.newArrayList(boolField,
                                                              charField,
                                                              stringField,
                                                              objectField,
                                                              byteField,
                                                              shortField,
                                                              integerField,
                                                              longField,
                                                              floatField,
                                                              doubleField);

    final Fields expectedFields = new Fields(Sets.newHashSet(expectedFieldList));
    final Set<Type> expectedTypes = Sets.newHashSet(fieldToType.values());

    Collections.sort(expectedFieldList);

    FieldsDescriptor fd = new FieldsDescriptor(fieldToType);

    Assert.assertEquals(expectedFieldList, fd.getFieldList());
    Assert.assertEquals(Sets.newHashSet(), fd.getCompressedTypes());
    Assert.assertEquals(expectedFields, fd.getFields());
    Assert.assertEquals(fieldToType, fd.getFieldToType());
    Assert.assertTrue(expectedTypes.containsAll(fd.getTypes()) &&
                      fd.getTypes().containsAll(expectedTypes));
    Assert.assertTrue(fd.getTypesList().containsAll(expectedTypes) &&
                      expectedTypes.containsAll(fd.getTypesList()));
  }

  @Test
  public void testCompressedTypes()
  {
    final Map<String, Type> fieldToType = Maps.newHashMap();
    fieldToType.put("a", Type.LONG);
    fieldToType.put("b", Type.LONG);
    fieldToType.put("c", Type.LONG);

    Set<Type> compressedTypes = Sets.newHashSet(Type.LONG);

    FieldsDescriptor fd = new FieldsDescriptor(fieldToType, compressedTypes);

    Assert.assertEquals(1, fd.getTypeToSize().getInt(Type.LONG));
    Assert.assertEquals(0, fd.getTypeToFieldToIndex().get(Type.LONG).getInt("a"));
    Assert.assertEquals(0, fd.getTypeToFieldToIndex().get(Type.LONG).getInt("b"));
    Assert.assertEquals(0, fd.getTypeToFieldToIndex().get(Type.LONG).getInt("c"));
  }

  @Test
  public void orderingTest()
  {
    final Map<String, Type> fieldToType = Maps.newHashMap();
    fieldToType.put("a", Type.LONG);
    fieldToType.put("b", Type.LONG);
    fieldToType.put("c", Type.LONG);

    final List<String> expectedList = Lists.newArrayList("a", "b", "c");

    FieldsDescriptor fd = new FieldsDescriptor(fieldToType);
    List<String> fieldList = fd.getTypeToFields().get(Type.LONG);

    Assert.assertEquals(expectedList, fieldList);

    Assert.assertEquals(0, fd.getTypeToFieldToIndex().get(Type.LONG).getInt("a"));
    Assert.assertEquals(1, fd.getTypeToFieldToIndex().get(Type.LONG).getInt("b"));
    Assert.assertEquals(2, fd.getTypeToFieldToIndex().get(Type.LONG).getInt("c"));
  }
}
