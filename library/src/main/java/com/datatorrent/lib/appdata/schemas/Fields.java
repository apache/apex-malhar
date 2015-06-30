/*
 * Copyright (c) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.appdata.schemas;

import java.io.Serializable;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a somewhat vacuous class for managing field names for AppData schemas and queries.
 * Its function is to maintain a set of field names with no null values. It also maintains a list
 * of field names to increase iteration speed.
 */
public class Fields implements Serializable
{
  private static final long serialVersionUID = 201506251241L;

  /**
   * Set of field names.
   */
  private Set<String> fields;
  /**
   * List of field names.
   */
  private List<String> fieldsList;

  /**
   * Creats an empty set of fields.
   */
  public Fields()
  {
  }

  /**
   * Creates a set of fields from the given collection of fields.
   * @param fields The collection object to create this {@link Fields} object from.
   */
  public Fields(Collection<String> fields)
  {
    this.fields = Sets.newHashSet();

    for(String field: fields) {
      Preconditions.checkNotNull(field);
      if(!this.fields.add(field)) {
        throw new IllegalArgumentException("Duplicate field: " + field);
      }
    }

    fieldsList = Lists.newArrayList();
    fieldsList.addAll(fields);
  }

  /**
   * This gets the set of fields managed by this {@link Fields} object.
   * @return The set of fields managed by this {@link Fields} object.
   */
  public Set<String> getFields()
  {
    return fields;
  }

  /**
   * Gets the list of fields managed by this {@link Fields} object.
   * @return The list of fields managed by this {@link Fields} object.
   */
  public List<String> getFieldsList()
  {
    return fieldsList;
  }

  @Override
  public String toString()
  {
    return "Fields{" + "fields=" + fields + '}';
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

  private static final Logger LOG = LoggerFactory.getLogger(Fields.class);
}
