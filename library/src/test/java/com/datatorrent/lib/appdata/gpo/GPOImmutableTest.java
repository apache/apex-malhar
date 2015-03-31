/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.gpo;

import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class GPOImmutableTest
{
  @Test
  public void simpleEqualsTest()
  {
    Map<String, Type> fieldToTypeA = Maps.newHashMap();
    fieldToTypeA.put("a", Type.LONG);
    fieldToTypeA.put("b", Type.STRING);

    FieldsDescriptor fdA = new FieldsDescriptor(fieldToTypeA);

    GPOMutable gpoA = new GPOMutable(fdA);
    gpoA.setField("a", 1L);
    gpoA.setField("b", "hello");

    Map<String, Type> fieldToTypeB = Maps.newHashMap();
    fieldToTypeB.put("a", Type.LONG);
    fieldToTypeB.put("b", Type.STRING);

    FieldsDescriptor fdB = new FieldsDescriptor(fieldToTypeB);

    GPOMutable gpoB = new GPOMutable(fdB);
    gpoB.setField("a", 1L);
    gpoB.setField("b", "hello");
    gpoB = new GPOImmutable(gpoB);

    Assert.assertEquals("The two gpos should equal", gpoA, gpoB);
    Assert.assertEquals("The two gpos should equal", gpoB, gpoA);
  }
}
