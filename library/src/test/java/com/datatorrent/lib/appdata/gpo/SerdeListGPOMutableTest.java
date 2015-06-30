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
package com.datatorrent.lib.appdata.gpo;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.Test;

import org.apache.commons.lang3.mutable.MutableInt;

import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;

public class SerdeListGPOMutableTest
{
  @Test
  public void testSerialization()
  {
    final Map<String, Type> fieldToType = Maps.newHashMap();

    final String tboolean = "tboolean";
    final Boolean tbooleanv = true;
    final Boolean tbooleanv1 = true;
    final String tchar = "tchar";
    final Character tcharv = 'A';
    final Character tcharv1 = 'b';
    final String tstring = "tstring";
    final String tstringv = "hello";
    final String tstringv1 = "hello1";
    final String tfloat = "tfloat";
    final Float tfloatv = 1.0f;
    final Float tfloatv1 = 2.0f;
    final String tdouble = "tdouble";
    final Double tdoublev = 2.0;
    final Double tdoublev1 = 3.0;
    final String tbyte = "tbyte";
    final Byte tbytev = 50;
    final Byte tbytev1 = 55;
    final String tshort = "tshort";
    final Short tshortv = 1000;
    final Short tshortv1 = 1300;
    final String tinteger = "tinteger";
    final Integer tintegerv = 100000;
    final Integer tintegerv1 = 133000;
    final String tlong = "tlong";
    final Long tlongv = 10000000000L;
    final Long tlongv1 = 10044400000L;

    fieldToType.put(tboolean, Type.BOOLEAN);
    fieldToType.put(tchar, Type.CHAR);
    fieldToType.put(tstring, Type.STRING);
    fieldToType.put(tfloat, Type.FLOAT);
    fieldToType.put(tdouble, Type.DOUBLE);
    fieldToType.put(tbyte, Type.BYTE);
    fieldToType.put(tshort, Type.SHORT);
    fieldToType.put(tinteger, Type.INTEGER);
    fieldToType.put(tlong, Type.LONG);

    FieldsDescriptor fd = new FieldsDescriptor(fieldToType);

    GPOMutable gpo = new GPOMutable(fd);

    gpo.setFieldGeneric(tboolean, tbooleanv);
    gpo.setFieldGeneric(tchar, tcharv);
    gpo.setField(tstring, tstringv);
    gpo.setFieldGeneric(tfloat, tfloatv);
    gpo.setFieldGeneric(tdouble, tdoublev);
    gpo.setFieldGeneric(tbyte, tbytev);
    gpo.setFieldGeneric(tshort, tshortv);
    gpo.setFieldGeneric(tinteger, tintegerv);
    gpo.setFieldGeneric(tlong, tlongv);

    GPOMutable gpo1 = new GPOMutable(fd);

    gpo1.setFieldGeneric(tboolean, tbooleanv1);
    gpo1.setFieldGeneric(tchar, tcharv1);
    gpo1.setField(tstring, tstringv1);
    gpo1.setFieldGeneric(tfloat, tfloatv1);
    gpo1.setFieldGeneric(tdouble, tdoublev1);
    gpo1.setFieldGeneric(tbyte, tbytev1);
    gpo1.setFieldGeneric(tshort, tshortv1);
    gpo1.setFieldGeneric(tinteger, tintegerv1);
    gpo1.setFieldGeneric(tlong, tlongv1);

    List<GPOMutable> mutables = Lists.newArrayList(gpo, gpo1);

    byte[] bytes = SerdeListGPOMutable.INSTANCE.serializeObject(mutables);
    MutableInt offset = new MutableInt(0);

    @SuppressWarnings("unchecked")
    List<GPOMutable> newMutables =
    (List<GPOMutable>) SerdeListGPOMutable.INSTANCE.deserializeObject(bytes, offset);

    Assert.assertEquals(mutables, newMutables);
    Assert.assertEquals(bytes.length, offset.intValue());
  }
}
