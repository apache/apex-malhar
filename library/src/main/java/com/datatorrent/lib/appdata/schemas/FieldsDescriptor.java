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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.objects.Object2IntLinkedOpenHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FieldsDescriptor
{
  private static final Logger logger = LoggerFactory.getLogger(FieldsDescriptor.class);

  private List<String> fieldList;
  private List<Type> typesList;
  private Set<Type> types;
  private Map<Type, Object2IntLinkedOpenHashMap<String>> typeToFieldToIndex;
  private Map<Type, List<String>> typeToFields;
  private Map<String, Type> fieldToType;
  private Set<Type> compressedTypes;
  private Object2IntLinkedOpenHashMap<Type> typeToSize;
  private transient Fields fields;

  public FieldsDescriptor()
  {
  }

  public FieldsDescriptor(Map<String, Type> fieldToType)
  {
    setFieldToType(fieldToType);
    compressedTypes = Sets.newHashSet();
    compressedTypes = Collections.unmodifiableSet(compressedTypes);
    initialize();
  }

  public FieldsDescriptor(Map<String, Type> fieldToType, Set<Type> compressedTypes)
  {
    setFieldToType(fieldToType);
    setCompressedTypes(compressedTypes);

    initialize();
  }

  public FieldsDescriptor(FieldsDescriptor fda, FieldsDescriptor fdb)
  {
    if(fda.getCompressedTypes().equals(
       fdb.getCompressedTypes())) {
      throw new IllegalArgumentException("The two fieldDescriptors have different compressedTypes\n" +
                                         "fda compressed types: " + fda.getCompressedTypes() + "\n" +
                                         "fdb compressed types: " + fdb.getCompressedTypes() + "\n");
    }

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
    setCompressedTypes(fda.getCompressedTypes());
    initialize();
  }

  private void initialize()
  {
    //TODO make this maps immutable
    typeToFieldToIndex = Maps.newHashMap();
    typeToFields = Maps.newHashMap();

    for(Map.Entry<String, Type> entry: fieldToType.entrySet()) {
      String field = entry.getKey();
      Type type = entry.getValue();

      List<String> fields = typeToFields.get(type);

      if(fields == null) {
        fields = Lists.newArrayList();
        typeToFields.put(type, fields);
      }

      fields.add(field);

      Object2IntLinkedOpenHashMap<String> fieldToIndex = getTypeToFieldToIndex().get(type);

      if(fieldToIndex == null) {
        fieldToIndex = new Object2IntLinkedOpenHashMap<String>();
        getTypeToFieldToIndex().put(type, fieldToIndex);
      }

      if(compressedTypes.contains(type)) {
        fieldToIndex.put(field, 0);
      }
      else {
        fieldToIndex.put(field, fields.size() - 1);
      }
    }

    //Types

    if(!typeToFields.isEmpty()) {
      types = EnumSet.copyOf(typeToFields.keySet());
    }
    else {
      types = Sets.newHashSet();
    }

    types = Collections.unmodifiableSet(types);

    //Types list
    typesList = Lists.newArrayList();
    typesList.addAll(types);
    typesList = Collections.unmodifiableList(typesList);

    //Field List

    fieldList = Lists.newArrayList();
    getFieldList().addAll(fieldToType.keySet());
    ((ArrayList<String>)getFieldList()).trimToSize();
    fieldList = Collections.unmodifiableList(getFieldList());

    //Array Sizes
    typeToSize = new Object2IntLinkedOpenHashMap<Type>();

    for(Map.Entry<Type, List<String>> entry: typeToFields.entrySet()) {
      Type type = entry.getKey();

      if(compressedTypes.contains(type)) {
        getTypeToSize().put(type, 1);
      }
      else {
        getTypeToSize().put(type, entry.getValue().size());
      }
    }
  }

  public Map<Type, Object2IntLinkedOpenHashMap<String>> getTypeToFieldToIndex()
  {
    return typeToFieldToIndex;
  }

  private void setFieldToType(Map<String, Type> fieldToType) {
    for(Map.Entry<String, Type> entry: fieldToType.entrySet()) {
      System.out.println("Field and Type: " + entry.getKey() + " " + entry.getValue());
      Preconditions.checkNotNull(entry.getKey());
      Preconditions.checkNotNull(entry.getValue());
    }

    //this.fieldToType = Collections.unmodifiableMap(Maps.newHashMap(fieldToType));
    this.fieldToType = Maps.newHashMap(fieldToType);
  }

  private void setCompressedTypes(Set<Type> compressedTypes)
  {
    for(Type type: compressedTypes) {
      Preconditions.checkNotNull(type);
    }

    this.compressedTypes = Collections.unmodifiableSet(compressedTypes);
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
    if(fields == null) {
      fields = new Fields(fieldToType.keySet());
    }

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

  public Map<Type, List<String>> getTypeToFields()
  {
    return typeToFields;
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

  /**
   * @return the types
   */
  public Set<Type> getTypes()
  {
    return types;
  }

  public List<Type> getTypesList()
  {
    return typesList;
  }

  /**
   * @return the fieldList
   */
  public List<String> getFieldList()
  {
    return fieldList;
  }

  /**
   * @return the compressedTypes
   */
  public Set<Type> getCompressedTypes()
  {
    return compressedTypes;
  }

  /**
   * @return the typeToSize
   */
  public Object2IntLinkedOpenHashMap<Type> getTypeToSize()
  {
    return typeToSize;
  }
}
