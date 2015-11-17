/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.dimensions;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import org.junit.Assert;

public class SimpleDataQueryDimensionalExpanderTest
{
  @Test
  public void simpleTest()
  {
    Map<String, Set<Object>> keyToValues = Maps.newHashMap();

    Set<Object> setA = Sets.newHashSet();
    keyToValues.put("a", setA);
    setA.add(1L);
    setA.add(3L);
    setA.add(5L);

    Set<Object> setB = Sets.newHashSet();
    keyToValues.put("b", setB);
    setB.add(2L);
    setB.add(4L);

    @SuppressWarnings("unchecked")
    SimpleDataQueryDimensionalExpander sdqde = new SimpleDataQueryDimensionalExpander((Map) keyToValues);

    Map<String, Type> fieldToType = Maps.newHashMap();
    fieldToType.put("a", Type.LONG);
    fieldToType.put("b", Type.LONG);

    FieldsDescriptor fd = new FieldsDescriptor(fieldToType);

    // Two star
    Map<String, Set<Object>> inputKeyToValues = Maps.newHashMap();
    inputKeyToValues.put("a", Sets.newHashSet());
    inputKeyToValues.put("b", Sets.newHashSet());

    List<GPOMutable> gpos = sdqde.createGPOs(inputKeyToValues, fd);

    Assert.assertEquals(6, gpos.size());

    // One star
    inputKeyToValues = Maps.newHashMap();
    Set<Object> inputSetA = Sets.newHashSet();
    inputSetA.add(1L);
    inputKeyToValues.put("a", inputSetA);
    inputKeyToValues.put("b", Sets.newHashSet());

    gpos = sdqde.createGPOs(inputKeyToValues, fd);

    Assert.assertEquals(2, gpos.size());

    // No star
    inputKeyToValues = Maps.newHashMap();
    inputSetA = Sets.newHashSet();
    inputSetA.add(1L);
    inputKeyToValues.put("a", inputSetA);

    Set<Object> inputSetB = Sets.newHashSet();
    inputSetB.add(2L);
    inputKeyToValues.put("b", inputSetB);

    gpos = sdqde.createGPOs(inputKeyToValues, fd);

    Assert.assertEquals(1, gpos.size());
  }
}
