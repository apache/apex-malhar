/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Set;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class FieldsDescriptor
{
  private Map<String, Type> fieldToType;
  private Fields fields;

  public FieldsDescriptor(Map<String, Type> fieldToType)
  {
    setFieldToType(fieldToType);
    initialize();
  }

  private void initialize()
  {
    Set<String> fieldSet = Sets.newHashSet();

    for(String field: fieldToType.keySet()) {
      fieldSet.add(field);
    }

    this.fields = new Fields(fieldSet);
  }

  private void setFieldToType(Map<String, Type> fieldToType) {
    for(Map.Entry<String, Type> entry: fieldToType.entrySet()) {
      Preconditions.checkNotNull(entry.getKey());
      Preconditions.checkNotNull(entry.getValue());
    }

    this.fieldToType = Maps.newHashMap(fieldToType);
  }

  public Type getType(String field) {
    return fieldToType.get(field);
  }

  public Fields getFields()
  {
    return fields;
  }

  public FieldsDescriptor getSubset(Fields fields)
  {
    Map<String, Type> newFieldToType = Maps.newHashMap();

    for(String field: fields.getFields()) {
      Type type = fieldToType.get(field);
      newFieldToType.put(field, type);
    }

    return new FieldsDescriptor(newFieldToType);
  }

  @Override
  public int hashCode()
  {
    int hash = 7;
    hash = 47 * hash + (this.fieldToType != null ? this.fieldToType.hashCode() : 0);
    hash = 47 * hash + (this.fields != null ? this.fields.hashCode() : 0);
    return hash;
  }

  @Override
  public boolean equals(Object obj)
  {
    if(obj == null) {
      return false;
    }
    if(getClass() != obj.getClass()) {
      return false;
    }
    final FieldsDescriptor other = (FieldsDescriptor)obj;
    if(this.fieldToType != other.fieldToType && (this.fieldToType == null || !this.fieldToType.equals(other.fieldToType))) {
      return false;
    }
    if(this.fields != other.fields && (this.fields == null || !this.fields.equals(other.fields))) {
      return false;
    }
    return true;
  }
}
