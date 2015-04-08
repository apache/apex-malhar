/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class Fields
{
  private Set<String> fields;
  private List<String> fieldsList;

  public Fields()
  {
  }

  public Fields(Set<String> fields)
  {
    setFields(fields);

    fieldsList = Lists.newArrayList();
    fieldsList.addAll(fields);
    fieldsList = Collections.unmodifiableList(fieldsList);
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

    setFields(fieldsSet);

    fieldsList = Lists.newArrayList();
    fieldsList.addAll(fields);
    fieldsList = Collections.unmodifiableList(fieldsList);
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

  /**
   * @return the fieldsList
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
}
