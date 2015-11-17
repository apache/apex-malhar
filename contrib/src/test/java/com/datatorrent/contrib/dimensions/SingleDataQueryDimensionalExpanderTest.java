/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.dimensions;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import com.datatorrent.lib.dimensions.DimensionsDescriptor;

public class SingleDataQueryDimensionalExpanderTest
{
  @Test
  public void simpleTest()
  {
    Map<String, Set<Object>> keyToValues = Maps.newHashMap();

    Set<Object> aValues = Sets.newHashSet();
    aValues.add(1L);
    keyToValues.put("a", aValues);

    Set<Object> bValues = Sets.newHashSet();
    bValues.add(2L);
    keyToValues.put("b", bValues);

    Map<String, Type> fieldToType = Maps.newHashMap();
    fieldToType.put("a", Type.LONG);
    fieldToType.put("b", Type.LONG);

    FieldsDescriptor fd = new FieldsDescriptor(fieldToType);
    List<GPOMutable> gpos = SingleDataQueryDimensionalExpander.INSTANCE.createGPOs(keyToValues, fd);
    GPOMutable gpo = gpos.get(0);

    Assert.assertEquals(1, gpos.size());
    Assert.assertEquals(1L, gpo.getFieldLong("a"));
    Assert.assertEquals(2L, gpo.getFieldLong("b"));
  }

  @Test
  public void emptyFieldsDescriptorTest()
  {
    Map<String, Type> fieldToType = Maps.newHashMap();

    fieldToType.put(DimensionsDescriptor.DIMENSION_TIME, Type.STRING);
    FieldsDescriptor fd = new FieldsDescriptor(fieldToType);

    Map<String, Set<Object>> keyToValues = Maps.newHashMap();
    List<GPOMutable> gpos = SingleDataQueryDimensionalExpander.INSTANCE.createGPOs(keyToValues, fd);

    Assert.assertEquals(1, gpos.size());
  }
}
