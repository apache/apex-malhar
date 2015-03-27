/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.gpo;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import com.google.common.collect.Maps;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
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
    
    gpo.setField(tboolean, tbooleanv);
    gpo.setField(tchar, tcharv);
    gpo.setField(tstring, tstringv);
    gpo.setField(tfloat, tfloatv);
    gpo.setField(tdouble, tdoublev);
    gpo.setField(tbyte, tbytev);
    gpo.setField(tshort, tshortv);
    gpo.setField(tinteger, tintegerv);
    gpo.setField(tlong, tlongv);

    int serializeLength = GPOUtils.serializedLength(gpo);

    Assert.assertEquals("The serialized byte length is incorrect.", totalBytes, serializeLength);
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

    logger.info("Correct total bytes {}.", totalBytes);

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

    gpo.setField(tboolean, tbooleanv);
    gpo.setField(tchar, tcharv);
    gpo.setField(tstring, tstringv);
    gpo.setField(tfloat, tfloatv);
    gpo.setField(tdouble, tdoublev);
    gpo.setField(tbyte, tbytev);
    gpo.setField(tshort, tshortv);
    gpo.setField(tinteger, tintegerv);
    gpo.setField(tlong, tlongv);

    byte[] gpoByte = GPOUtils.serialize(gpo);
    logger.info("GPO num bytes: {}", gpoByte.length);
    GPOMutable dgpo = GPOUtils.deserialize(fd, gpoByte, 0);

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
}
