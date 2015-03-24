/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class FieldsDescriptor
{
  private static final Logger logger = LoggerFactory.getLogger(FieldsDescriptor.class);

  private Map<String, Type> fieldToType;
  private List<String> fieldOrder;
  private Fields fields;

  public FieldsDescriptor()
  {
  }

  public FieldsDescriptor(Map<String, Type> fieldToType)
  {
    setFieldToType(fieldToType);
    initialize();
  }

  public FieldsDescriptor(FieldsDescriptor fda, FieldsDescriptor fdb)
  {
    if(!Collections.disjoint(fda.getFields().getFields(),
                             fdb.getFields().getFields()))
    {
      throw new IllegalArgumentException("The two provided field descriptors must have disjoint sets of fields.");
    }

    fieldToType = Maps.newHashMap();

    for(String field: fda.getFields().getFields()) {
      fieldToType.put(field, fda.getType(field));
    }

    for(String field: fdb.getFields().getFields()) {
      fieldToType.put(field, fdb.getType(field));
    }

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
    fieldOrder = Lists.newArrayList();
    fieldOrder.addAll(fieldSet);

    //fieldOrder = Collections.unmodifiableList(fieldOrder);
  }

  private void setFieldToType(Map<String, Type> fieldToType) {
    for(Map.Entry<String, Type> entry: fieldToType.entrySet()) {
      System.out.println("Field and Type: " + entry.getKey() + " " + entry.getValue());
      Preconditions.checkNotNull(entry.getKey());
      Preconditions.checkNotNull(entry.getValue());
    }

    this.fieldToType = Collections.unmodifiableMap(Maps.newHashMap(fieldToType));
  }

  public Map<String, Type> getFieldToType()
  {
    return fieldToType;
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

  public List<String> getFieldOrder()
  {
    return fieldOrder;
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
