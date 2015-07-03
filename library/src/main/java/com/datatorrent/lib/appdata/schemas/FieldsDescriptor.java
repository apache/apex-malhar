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

import it.unimi.dsi.fastutil.objects.Object2IntLinkedOpenHashMap;
import java.io.Serializable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.appdata.gpo.Serde;
import com.datatorrent.lib.appdata.gpo.SerdeObjectPayloadFix;

/**
 * This class manages the storage of fields in app data. It is used in {@link GPOMutable} objects
 * to map field names to values in order to respond to queries, it also serves as a schema which is
 * used in the serialization of {@link GPOMutable} objects in order to ensure consistent serialization
 * and deserialization of data.
 */
public class FieldsDescriptor implements Serializable
{
  private static final long serialVersionUID = 201506251228L;

  /**
   * This is a list of all of the fields managed by this {@link FieldsDescriptor} object.
   */
  private List<String> fieldList;
  /**
   * This is a list of all of the types managed by this {@link FieldsDescriptor} object.
   */
  private List<Type> typesList;
  /**
   * This is a set of all the fields managed by this {@link FieldsDescriptor} object.
   */
  private Fields fields;
  /**
   * This is a set of all of the types managed by this {@link FieldsDescriptor} object.
   */
  private Set<Type> types;

  private Map<Type, Object2IntLinkedOpenHashMap<String>> typeToFieldToIndex;
  /**
   * A Map from the type to the list of fields corresponding to that type. The list of fields
   * corresponding to a type also represents the order in which the fields are stored in
   * both a {@link GPOMutable} object and the order of fields when {@link GPOMutable} objects
   * are serialized.
   */
  private Map<Type, List<String>> typeToFields;
  /**
   * This is the field to type mapping for this {@link FieldsDesciptor} object.
   */
  private Map<String, Type> fieldToType;
  /**
   * This is the set of all the compressed types for this {@link FieldsDescriptor} object.
   */
  private Set<Type> compressedTypes;
  /**
   * This is a map from the type of a field to the number of values to store for fields of
   * that type. Typically the number of values to store for a type is the same as the number
   * of fields of that type, but if type compression is enabled for that field then the number
   * of values stored for that type is 1.
   */
  private Object2IntLinkedOpenHashMap<Type> typeToSize;

  private Serde[] serdes;
  private SerdeObjectPayloadFix serdePayloadFix;

  /**
   * This constructor is used for serialization.
   */
  private FieldsDescriptor()
  {
    //For kryo
  }

  /**
   * This creates a {@link FieldsDescriptor} with the given field to type map.
   * @param fieldToType A mapping from field names to the type of the field.
   */
  public FieldsDescriptor(Map<String, Type> fieldToType)
  {
    setFieldToType(fieldToType);
    compressedTypes = Sets.newHashSet();
    initialize();
  }

  /**
   * This creates a {@link FieldsDescriptor} with the given field to type map.
   * @param fieldToType A mapping from field names to the type of the field.
   * @param fieldToSerdeObject A mapping from field names to the corresponding serde object.
   */
  public FieldsDescriptor(Map<String, Type> fieldToType,
                          Map<String, Serde> fieldToSerdeObject)
  {
    setFieldToType(fieldToType);
    compressedTypes = Sets.newHashSet();

    initialize();

    List<String> fieldNames = typeToFields.get(Type.OBJECT);

    if(fieldNames == null) {
      throw new IllegalArgumentException("There are no fields of type " + Type.OBJECT +
                                         " in this fieldsdescriptor");
    }
    else {
      serdes = new Serde[fieldNames.size()];

      //Insert serdes in corresponding order
      for(int index = 0;
          index < fieldNames.size();
          index++) {
        String fieldName = fieldNames.get(index);
        Serde serdeObject = fieldToSerdeObject.get(fieldName);
        if(serdeObject == null) {
          throw new IllegalArgumentException("The field "
                                             + fieldName
                                             + " doesn't have a serde object.");
        }

        serdes[index] = serdeObject;
      }
    }
  }

  public FieldsDescriptor(Map<String, Type> fieldToType,
                          Map<String, Serde> fieldToSerdeObject,
                          SerdeObjectPayloadFix serdePayloadFix)
  {
    this(fieldToType, fieldToSerdeObject);
    this.serdePayloadFix = serdePayloadFix;
  }

  /**
   * This creates a field descriptor with the given field to type map.
   * <br/><br/>
   * <b>Note:</b> Compressed types are currently ignored and will be implemented in the future.
   * @param fieldToType A mapping from field names to the type of the field.
   * @param compressedTypes The types, which will have compressed values.
   */
  public FieldsDescriptor(Map<String, Type> fieldToType, Set<Type> compressedTypes)
  {
    setFieldToType(fieldToType);
    setCompressedTypes(compressedTypes);

    initialize();
  }

  /**
   * This is a helper method, which initializes all the data structures for a
   * {@link FieldsDescriptor} object.
   */
  private void initialize()
  {
    //TODO make this maps immutable
    typeToFieldToIndex = Maps.newHashMap();
    typeToFields = Maps.newHashMap();

    for(Map.Entry<String, Type> entry: fieldToType.entrySet()) {
      String field = entry.getKey();
      Type type = entry.getValue();

      List<String> fieldsList = typeToFields.get(type);

      if(fieldsList == null) {
        fieldsList = Lists.newArrayList();
        typeToFields.put(type, fieldsList);
      }

      fieldsList.add(field);
    }

    //ensure consistent ordering of fields
    for(Map.Entry<Type, List<String>> entry: typeToFields.entrySet()) {
      Type type = entry.getKey();
      List<String> tempFields = entry.getValue();

      Collections.sort(tempFields);
      Object2IntLinkedOpenHashMap<String> fieldToIndex = new Object2IntLinkedOpenHashMap<String>();

      for(int index = 0;
          index < tempFields.size();
          index++) {
        String field = tempFields.get(index);

        if(compressedTypes.contains(type)) {
          fieldToIndex.put(field, 0);
        }
        else {
          fieldToIndex.put(field, index);
        }
      }

      typeToFieldToIndex.put(type, fieldToIndex);
    }

    //Types

    if(!typeToFields.isEmpty()) {
      types = EnumSet.copyOf(typeToFields.keySet());
    }
    else {
      types = Sets.newHashSet();
    }

    //Types list
    typesList = Lists.newArrayList();
    typesList.addAll(types);

    //Field List
    fieldList = Lists.newArrayList();
    fieldList.addAll(fieldToType.keySet());
    ((ArrayList<String>)fieldList).trimToSize();
    Collections.sort(fieldList);

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

  /**
   * Gets the type to field to index map.
   * @return The type to field to index map.
   */
  public Map<Type, Object2IntLinkedOpenHashMap<String>> getTypeToFieldToIndex()
  {
    return typeToFieldToIndex;
  }

  /**
   * This is a helper method which sets and validates the field to type
   * map.
   * @param fieldToType The field to type map to set for this {@link FieldsDescriptor}
   * object.
   */
  private void setFieldToType(Map<String, Type> fieldToType) {
    for(Map.Entry<String, Type> entry: fieldToType.entrySet()) {
      Preconditions.checkNotNull(entry.getKey());
      Preconditions.checkNotNull(entry.getValue());
    }

    this.fieldToType = Maps.newHashMap(fieldToType);
  }

  /**
   * This is a helper method to set and validate compressed types.
   * @param compressedTypes The compressed types to set.
   */
  private void setCompressedTypes(Set<Type> compressedTypes)
  {
    for(Type type: compressedTypes) {
      Preconditions.checkNotNull(type);
    }

    this.compressedTypes = Sets.newHashSet(compressedTypes);
  }

  /**
   * Gets the field to type map for this {@link FieldsDescriptor} object.
   * @return The type map for this {@link FieldsDescriptor} object.
   */
  public Map<String, Type> getFieldToType()
  {
    return fieldToType;
  }

  /**
   * The type of the given field.
   * @param field The field whose type needs to be looked up.
   * @return The type of the given field.
   */
  public Type getType(String field)
  {
    return fieldToType.get(field);
  }

  /**
   * Gets the set of fields managed by this {@link FieldsDescriptor} object.
   * @return The set of fields managed by this {@link FieldsDescriptor object.
   */
  public Fields getFields()
  {
    if(fields == null) {
      fields = new Fields(fieldToType.keySet());
    }

    return fields;
  }

  /**
   * This method creates a new {@link FieldsDescriptor} object which only includes
   * the given set of fields.
   * @param fields The fields to include in the new {@link FieldsDescriptor}.
   * @return The fields to include in the new {@link FieldsDescriptor}.
   */
  public FieldsDescriptor getSubset(Fields fields)
  {
    Map<String, Type> newFieldToType = Maps.newHashMap();

    for(String field: fields.getFields()) {
      Type type = fieldToType.get(field);
      newFieldToType.put(field, type);
    }

    return new FieldsDescriptor(newFieldToType);
  }

  /**
   * Returns a map from type to a list of all the fields with that type.
   * @return A map from type to a list of all the fields with that type.
   */
  public Map<Type, List<String>> getTypeToFields()
  {
    return typeToFields;
  }

  /**
   * Returns set of types of all the fields managed by this {@link FieldsDescriptor} object.
   * @return The set of types of all the fields managed by this {@link FieldsDescriptor} object.
   */
  public Set<Type> getTypes()
  {
    return types;
  }

  /**
   * Returns the list of types of all the fields managed by this {@link FieldsDescriptor} object.
   * The content of this list will be the same as the set, this method is provided because iterating
   * over lists is faster than iterating over sets.
   * @return The list of types of all the fields managed by this {@link FieldsDescriptor} object.
   */
  public List<Type> getTypesList()
  {
    return typesList;
  }

  /**
   * Returns a list of all of the fields represented by the {@link FieldsDescriptor} object.
   * This method is provided because it is faster to iterate over lists.
   * @return the fieldList
   */
  public List<String> getFieldList()
  {
    return fieldList;
  }

  /**
   * Gets the types whose corresponding fields will be compressed to share the same value.
   * @return The types whose corresponding fields will be compressed to share the same value.
   */
  public Set<Type> getCompressedTypes()
  {
    return compressedTypes;
  }

  /**
   * Gets the mapping from the type of a field to the number of values to store for that type.
   * The number of of values to store for a type is usually the number of fields of that type,
   * but if there is type compression enabled for a type, the number of values to store for
   * that type will be 1.
   * @return A map from a type to the number of values to store for that type.
   */
  public Object2IntLinkedOpenHashMap<Type> getTypeToSize()
  {
    return typeToSize;
  }

  public Serde[] getSerdes()
  {
    return serdes;
  }

  public SerdeObjectPayloadFix getSerdePayloadFix()
  {
    return this.serdePayloadFix;
  }

  @Override
  public int hashCode()
  {
    int hash = 7;
    hash = 53 * hash + (this.fieldToType != null ? this.fieldToType.hashCode() : 0);
    hash = 53 * hash + (this.compressedTypes != null ? this.compressedTypes.hashCode() : 0);
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
    if(this.compressedTypes != other.compressedTypes && (this.compressedTypes == null || !this.compressedTypes.equals(other.compressedTypes))) {
      return false;
    }
    return true;
  }

  @Override
  public String toString()
  {
    return "FieldsDescriptor{" + "fieldToType=" + fieldToType + ", compressedTypes=" + compressedTypes + '}';
  }

  private static final Logger LOG = LoggerFactory.getLogger(FieldsDescriptor.class);
}
