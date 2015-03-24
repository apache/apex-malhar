/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class Fields
{
  private Set<String> fields;

  public Fields(Set<String> fields)
  {
    setFields(fields);
  }

  public Fields(List<String> fields)
  {
    Set<String> fieldsSet = Sets.newHashSet();

    for(String field: fields) {
      Preconditions.checkNotNull(field);
      if(!fieldsSet.add(field)) {
        throw new IllegalArgumentException("Duplicate field: " + field);
      }
    }
  }

  private void setFields(Set<String> fields)
  {
    for(String field: fields) {
      Preconditions.checkNotNull(field);
    }

    //this.fields = Collections.unmodifiableSet(Sets.newHashSet(fields));
    this.fields = Sets.newHashSet(fields);
  }

  public Set<String> getFields()
  {
    return fields;
  }

  @Override
  public int hashCode()
  {
    int hash = 7;
    hash = 43 * hash + (this.fields != null ? this.fields.hashCode() : 0);
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
    final Fields other = (Fields)obj;
    if(this.fields != other.fields && (this.fields == null || !this.fields.equals(other.fields))) {
      return false;
    }
    return true;
  }
}
