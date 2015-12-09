/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.appdata.schemas;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datatorrent.lib.appdata.gpo.GPOMutable;

public interface DataQueryDimensionalExpander
{
  public List<GPOMutable> createGPOs(Map<String, Set<Object>> keyToValues, FieldsDescriptor fd);
}
