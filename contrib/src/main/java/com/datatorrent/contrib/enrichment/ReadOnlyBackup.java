package com.datatorrent.contrib.enrichment;

import java.util.List;
import java.util.Map;

public abstract class ReadOnlyBackup implements EnrichmentBackup
{
  protected transient List<String> includeFields;
  protected transient List<String> lookupFields;

  @Override public void put(Object key, Object value)
  {
    throw new RuntimeException("Not supported operation");
  }

  @Override public void putAll(Map<Object, Object> m)
  {
    throw new RuntimeException("Not supported operation");
  }

  @Override public void remove(Object key)
  {
    throw new RuntimeException("Not supported operation");
  }

  @Override public void setFields(List<String> lookupFields,List<String> includeFields)
  {
    this.includeFields = includeFields;
    this.lookupFields = lookupFields;
  }
}
