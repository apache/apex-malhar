/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.dimensions;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.DataQueryDimensionalExpander;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;

public class SingleDataQueryDimensionalExpander implements DataQueryDimensionalExpander
{
  public static final SingleDataQueryDimensionalExpander INSTANCE = new SingleDataQueryDimensionalExpander();

  @Override
  public List<GPOMutable> createGPOs(Map<String, Set<Object>> keyToValues, FieldsDescriptor fd)
  {
    List<GPOMutable> keys = Lists.newArrayList();
    GPOMutable gpo = new GPOMutable(fd);
    keys.add(gpo);

    for (Map.Entry<String, Set<Object>> entry : keyToValues.entrySet()) {
      Preconditions.checkArgument(entry.getValue().size() == 1, "There should only be one value for the key.");
      gpo.setFieldGeneric(entry.getKey(), entry.getValue().iterator().next());
    }

    return keys;
  }
}
