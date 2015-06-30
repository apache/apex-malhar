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

import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.mutable.MutableInt;

import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;

public class GPOUtilsTest
{
  private static final Logger logger = LoggerFactory.getLogger(GPOUtilsTest.class);

  @Test
  public void testSerializationLength()
  {
    final Map<String, Type> fieldToType = Maps.newHashMap();

    final String tboolean = "tboolean";
    final Boolean tbooleanv = true;
    final String tchar = "tchar";
    final Character tcharv = 'A';
    final String tstring = "tstring";
    final String tstringv = "hello";
    final String tfloat = "tfloat";
    final Float tfloatv = 1.0f;
    final String tdouble = "tdouble";
    final Double tdoublev = 2.0;
    final String tbyte = "tbyte";
    final Byte tbytev = 50;
    final String tshort = "tshort";
    final Short tshortv = 1000;
    final String tinteger = "tinteger";
    final Integer tintegerv = 100000;
    final String tlong = "tlong";
    final Long tlongv = 10000000000L;

    int totalBytes = 1 //boolean
                     + 2 //char
                     + 4 + tstringv.getBytes().length //string
                     + 4 //float
                     + 8 //double
                     + 1 //byte
                     + 2 //short
                     + 4 //int
                     + 8; //long

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

    int serializeLength = GPOUtils.serializedLength(gpo);

    Assert.assertEquals("The serialized byte length is incorrect.", totalBytes, serializeLength);
  }

  @Test
  public void testCornerCaseSerializationLegnth()
  {
    final Map<String, Type> fieldToType = Maps.newHashMap();
    fieldToType.put("a", Type.OBJECT);
    fieldToType.put("b", Type.OBJECT);

    GPOMutable gpo = new GPOMutable(new FieldsDescriptor(fieldToType));

    int serializeLength = GPOUtils.serializedLength(gpo);

    Assert.assertEquals(0, serializeLength);
  }

  @Test
  public void simpleSerializeDeserializeTest()
  {
    final Map<String, Type> fieldToType = Maps.newHashMap();

    final String tboolean = "tboolean";
    final Boolean tbooleanv = true;
    final String tchar = "tchar";
    final Character tcharv = 'A';
    final String tstring = "tstring";
    final String tstringv = "hello";
    final String tfloat = "tfloat";
    final Float tfloatv = 1.0f;
    final String tdouble = "tdouble";
    final Double tdoublev = 2.0;
    final String tbyte = "tbyte";
    final Byte tbytev = 50;
    final String tshort = "tshort";
    final Short tshortv = 1000;
    final String tinteger = "tinteger";
    final Integer tintegerv = 100000;
    final String tlong = "tlong";
    final Long tlongv = 10000000000L;

    int totalBytes = 1 //boolean
                     + 2 //char
                     + 4 + tstringv.getBytes().length //string
                     + 4 //float
                     + 8 //double
                     + 1 //byte
                     + 2 //short
                     + 4 //int
                     + 8; //long

    logger.debug("Correct total bytes {}.", totalBytes);

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

    GPOByteArrayList byteArrayList = new GPOByteArrayList();
    byte[] gpoByte = GPOUtils.serialize(gpo, byteArrayList);
    logger.debug("GPO num bytes: {}", gpoByte.length);
    GPOMutable dgpo = GPOUtils.deserialize(fd, gpoByte, new MutableInt(0));

    Assert.assertEquals("Values must equal", tbooleanv, dgpo.getField(tboolean));
    Assert.assertEquals("Values must equal", tcharv, dgpo.getField(tchar));
    Assert.assertEquals("Values must equal", tstringv, dgpo.getField(tstring));
    Assert.assertEquals("Values must equal", tfloatv, dgpo.getField(tfloat));
    Assert.assertEquals("Values must equal", tdoublev, dgpo.getField(tdouble));
    Assert.assertEquals("Values must equal", tbytev, dgpo.getField(tbyte));
    Assert.assertEquals("Values must equal", tshortv, dgpo.getField(tshort));
    Assert.assertEquals("Values must equal", tintegerv, dgpo.getField(tinteger));
    Assert.assertEquals("Values must equal", tlongv, dgpo.getField(tlong));
  }

  @Test
  public void validDeserializeTest() throws Exception
  {
    final Map<String, Type> fieldToType = Maps.newHashMap();

    final String tboolean = "tboolean";
    final Boolean tbooleanv = true;
    final String tchar = "tchar";
    final Character tcharv = 'A';
    final String tstring = "tstring";
    final String tstringv = "hello";
    final String tfloat = "tfloat";
    final Float tfloatv = 1.0f;
    final String tdouble = "tdouble";
    final Double tdoublev = 2.0;
    final String tbyte = "tbyte";
    final Byte tbytev = 50;
    final String tshort = "tshort";
    final Short tshortv = 1000;
    final String tinteger = "tinteger";
    final Integer tintegerv = 100000;
    final String tlong = "tlong";
    final Long tlongv = 10000000000L;

    fieldToType.put(tboolean, Type.BOOLEAN);
    fieldToType.put(tchar, Type.CHAR);
    fieldToType.put(tstring, Type.STRING);
    fieldToType.put(tfloat, Type.FLOAT);
    fieldToType.put(tdouble, Type.DOUBLE);
    fieldToType.put(tbyte, Type.BYTE);
    fieldToType.put(tshort, Type.SHORT);
    fieldToType.put(tinteger, Type.INTEGER);
    fieldToType.put(tlong, Type.LONG);

    JSONObject jo = new JSONObject();
    jo.put(tboolean, tbooleanv);
    jo.put(tchar, tcharv);
    jo.put(tstring, tstringv);
    jo.put(tfloat, tfloatv);
    jo.put(tdouble, tdoublev);
    jo.put(tbyte, tbytev);
    jo.put(tshort, tshortv);
    jo.put(tinteger, tintegerv);
    jo.put(tlong, tlongv);

    String json = jo.toString(2);
    logger.debug("Input json: {}", json);

    GPOMutable gpom = GPOUtils.deserialize(new FieldsDescriptor(fieldToType), jo);

    Assert.assertEquals("Results must equal", tbooleanv, gpom.getField(tboolean));
    Assert.assertEquals("Results must equal", tcharv, gpom.getField(tchar));
    Assert.assertEquals("Results must equal", tfloatv, gpom.getField(tfloat));
    Assert.assertEquals("Results must equal", tdoublev, gpom.getField(tdouble));
    Assert.assertEquals("Results must equal", tbytev, gpom.getField(tbyte));
    Assert.assertEquals("Results must equal", tshortv, gpom.getField(tshort));
    Assert.assertEquals("Results must equal", tintegerv, gpom.getField(tinteger));
    Assert.assertEquals("Results must equal", tlongv, gpom.getField(tlong));
  }

  @Test
  public void objectSerdeTest()
  {
    Map<String, Type> fieldToTypeKey = Maps.newHashMap();
    fieldToTypeKey.put("publisher", Type.STRING);
    fieldToTypeKey.put("advertiser", Type.STRING);
    FieldsDescriptor fdkey = new FieldsDescriptor(fieldToTypeKey);

    Map<String, Type> fieldToTypeAgg = Maps.newHashMap();
    fieldToTypeAgg.put("clicks", Type.LONG);
    fieldToTypeAgg.put("impressions", Type.LONG);
    FieldsDescriptor fdagg = new FieldsDescriptor(fieldToTypeAgg);

    Map<String, Type> fieldToType = Maps.newHashMap();
    fieldToType.put("fdkeys", Type.OBJECT);
    fieldToType.put("fdvalues", Type.OBJECT);
    fieldToType.put("keys", Type.OBJECT);
    fieldToType.put("values", Type.OBJECT);

    Map<String, Serde> fieldToSerde = Maps.newHashMap();
    fieldToSerde.put("fdkeys", SerdeFieldsDescriptor.INSTANCE);
    fieldToSerde.put("fdvalues", SerdeFieldsDescriptor.INSTANCE);
    fieldToSerde.put("keys", SerdeListGPOMutable.INSTANCE);
    fieldToSerde.put("values", SerdeListGPOMutable.INSTANCE);

    FieldsDescriptor metaDataFD = new FieldsDescriptor(fieldToType,
                                                       fieldToSerde,
                                                       new PayloadFix());

    GPOMutable gpo = new GPOMutable(metaDataFD);

    GPOMutable key1 = new GPOMutable(fdkey);
    key1.setField("publisher", "google");
    key1.setField("advertiser", "safeway");
    GPOMutable key2 = new GPOMutable(fdkey);
    key2.setField("publisher", "twitter");
    key2.setField("advertiser", "blockbuster");

    GPOMutable agg1 = new GPOMutable(fdagg);
    agg1.setField("clicks", 10L);
    agg1.setField("impressions", 11L);
    GPOMutable agg2 = new GPOMutable(fdagg);
    agg2.setField("clicks", 5L);
    agg2.setField("impressions", 4L);

    List<GPOMutable> keys = Lists.newArrayList(key1, key2);
    List<GPOMutable> aggs = Lists.newArrayList(agg1, agg2);

    gpo.getFieldsObject()[0] = fdkey;
    gpo.getFieldsObject()[1] = fdagg;
    gpo.getFieldsObject()[2] = keys;
    gpo.getFieldsObject()[3] = aggs;

    GPOByteArrayList bal = new GPOByteArrayList();
    byte[] serialized = GPOUtils.serialize(gpo, bal);

    GPOMutable newGPO = GPOUtils.deserialize(metaDataFD, serialized, new MutableInt(0));
    newGPO.setFieldDescriptor(metaDataFD);

    Assert.assertEquals(gpo, newGPO);
    Assert.assertArrayEquals(gpo.getFieldsObject(), newGPO.getFieldsObject());
  }

  private static final Logger LOG = LoggerFactory.getLogger(GPOUtilsTest.class);

  public static class PayloadFix implements SerdeObjectPayloadFix
  {
    @Override
    public void fix(Object[] objects)
    {
      FieldsDescriptor keyfd = (FieldsDescriptor) objects[0];
      FieldsDescriptor valuefd = (FieldsDescriptor) objects[1];

      @SuppressWarnings("unchecked")
      List<GPOMutable> keyMutables = (List<GPOMutable>) objects[2];
      @SuppressWarnings("unchecked")
      List<GPOMutable> aggregateMutables = (List<GPOMutable>) objects[3];

      fix(keyfd, keyMutables);
      fix(valuefd, aggregateMutables);
    }

    private void fix(FieldsDescriptor fd, List<GPOMutable> mutables)
    {
      for(int index = 0;
          index < mutables.size();
          index++) {
        mutables.get(index).setFieldDescriptor(fd);
      }
    }
  }
}
