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
