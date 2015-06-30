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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

/**
 * This enum is used to represent data types throughout AppData Framework.
 */
public enum Type implements Serializable
{
  /**
   * Boolean data type.
   */
  BOOLEAN("boolean", 1, JSONType.BOOLEAN, Boolean.class,
          Collections.unmodifiableSet(new HashSet<Type>())),
  /**
   * String data type.
   */
  STRING("string", -1, JSONType.STRING, String.class,
         Collections.unmodifiableSet(new HashSet<Type>())),
  /**
   * Char data type.
   */
  CHAR("char", 2, JSONType.STRING, Character.class,
       ImmutableSet.of(STRING)),
  /**
   * Double data type.
   */
  DOUBLE("double", 8, JSONType.NUMBER, Double.class,
         Collections.unmodifiableSet(new HashSet<Type>())),
  /**
   * Float data type.
   */
  FLOAT("float", 4, JSONType.NUMBER, Float.class,
        ImmutableSet.of(DOUBLE)),
  /**
   * Long data type.
   */
  LONG("long", 8, JSONType.NUMBER, Long.class,
       Collections.unmodifiableSet(new HashSet<Type>())),
  /**
   * Integer data type.
   */
  INTEGER("integer", 4, JSONType.NUMBER, Integer.class,
          ImmutableSet.of(LONG)),
  /**
   * Short data type.
   */
  SHORT("short", 2, JSONType.NUMBER, Short.class,
        ImmutableSet.of(INTEGER, LONG)),
  /**
   * Byte data type.
   */
  BYTE("byte", 1, JSONType.NUMBER, Byte.class,
       ImmutableSet.of(SHORT, INTEGER, LONG)),
  /**
   * Object data type.
   */
  OBJECT("object", -1, JSONType.INVALID, Object.class,
          Collections.unmodifiableSet(new HashSet<Type>()));

  /**
   * A set containing all the numeric types.
   */
  public static final Set<Type> NUMERIC_TYPES = ImmutableSet.of(BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE);
  /**
   * A set containing all the non numeric types.
   */
  public static final Set<Type> NON_NUMERIC_TYPES = ImmutableSet.of(BOOLEAN, CHAR, STRING);
  /**
   * A map from the names to the types.
   */
  public static final Map<String, Type> NAME_TO_TYPE;
  /**
   * A map from a class representing the type to the type itself.
   */
  public static final Map<Class<?>, Type> CLASS_TO_TYPE;

  static {
    Map<String, Type> nameToType = Maps.newHashMap();

    for(Type type: Type.values()) {
      nameToType.put(type.getName(), type);
    }

    NAME_TO_TYPE = Collections.unmodifiableMap(nameToType);

    Map<Class<?>, Type> clazzToType = Maps.newHashMap();

    for(Type type: Type.values()) {
      clazzToType.put(type.getClazz(), type);
    }

    CLASS_TO_TYPE = Collections.unmodifiableMap(clazzToType);
  }

  /**
   * The name of the type.
   */
  private final String name;
  /**
   * The type of the corresponding JSON element.
   */
  private final JSONType jsonType;
  /**
   * Class of the corresponding Java type.
   */
  private final Class<?> clazz;
  /**
   * In the case of numeric types, there may be a "higher" type. For example
   * ints can be promoted to longs. This set holds all of the types that this
   * type could be promoted to. If this type cannot be promoted then this will be empty.
   */
  private final Set<Type> higherTypes;
  /**
   * The number of bytes required to store a value of this type. If a value of
   * this type can be of variable length then this would be -1.
   */
  private final int byteSize;

  /**
   * Creates a type enum.
   * @param name The name of the type.
   * @param byteSize The number of bytes a value of this type would occupy. -1 if variable length.
   * @param jsonType The type of corresponding json values.
   * @param clazz The Class of the corresponding Java type.
   * @param higherTypes The set of types to which this type can be promoted.
   */
  Type(String name,
       int byteSize,
       JSONType jsonType,
       Class<?> clazz,
       Set<Type> higherTypes)
  {
    this.name = name;
    this.byteSize = byteSize;
    this.jsonType = jsonType;
    this.clazz = clazz;
    this.higherTypes = higherTypes;
  }

  /**
   * Gets the name of the type.
   * @return The name of the type.
   */
  public String getName()
  {
    return name;
  }

  /**
   * Gets the byte size of the value.
   * @return The byte size of the value.
   */
  public int getByteSize()
  {
    return byteSize;
  }

  /**
   * Gets the corresponding JSONType of this type.
   * @return The corresponding JSONType of this type.
   */
  public JSONType getJSONType()
  {
    return jsonType;
  }

  /**
   * Gets the Java class corresponding to this type.
   * @return The Java class corresponding to this type.
   */
  public Class<?> getClazz()
  {
    return clazz;
  }

  /**
   * Gets the set of types to which this type can be promoted.
   * @return The set of types to which this type can be promoted.
   */
  public Set<Type> getHigherTypes()
  {
    return higherTypes;
  }

  /**
   * Determines if the given type is a higher type of this type.
   * @param type The type which may or not be a parent of this type.
   * @return True if the given type is a higher type of this type. False otherwise.
   */
  public boolean isChildOf(Type type)
  {
    return higherTypes.contains(type);
  }

  /**
   * Determines if one of the given types is a child of the other.
   * @param a This type will be checked to see if it is a child of b.
   * @param b This type will be checked to see if it is a child of a.
   * @return True if one of the given types is a child of the other. False otherwise.
   */
  public static boolean areRelated(Type a, Type b)
  {
    return a == b || a.isChildOf(b) || b.isChildOf(a);
  }

  /**
   * Gets the type corresponding to the given name.
   * @param name The name of the type.
   * @return The type corresponding to the name or null if there is no type
   * corresponding to the given name.
   */
  public static Type getType(String name)
  {
    return NAME_TO_TYPE.get(name);
  }

  public static Type getTypeEx(String name)
  {
    Type type = getType(name);

    Preconditions.checkArgument(type != null,
                                name + " is not a valid type.");

    return type;
  }

  /**
   * Promotes the given object from the "from" type to the "to" type.
   * @param from The current type of the given object.
   * @param to The desired type for the given object.
   * @param o The object whose type must be converted.
   * @return The result of promoting the given object o to the "to" type.
   */
  public static Object promote(Type from, Type to, Object o)
  {
    if(from == to) {
      return o;
    }

    Preconditions.checkArgument(!(from == Type.BOOLEAN
                                  || from == Type.CHAR
                                  || from == Type.LONG
                                  || from == Type.DOUBLE),
                                "Cannot convert "
                                + Type.BOOLEAN.getName() + ", "
                                + Type.CHAR.getName() + ", "
                                + Type.LONG.getName() + ", or "
                                + Type.DOUBLE + " to a larger type.");

    Preconditions.checkArgument(from.getHigherTypes().contains(to),
                                from.getName() + " cannot be promoted to " + to.getName());

    if(from == Type.FLOAT && to == Type.DOUBLE) {
      return (Double)((Float)o).doubleValue();
    }

    if(from == Type.BYTE) {
      if(to == Type.SHORT) {
        return (Short)((Byte)o).shortValue();
      }
      else if(to == Type.INTEGER) {
        return (Integer)((Byte)o).intValue();
      }
      else if(to == Type.LONG) {
        return (Long)((Byte)o).longValue();
      }
    }

    if(from == Type.SHORT) {
      if(to == Type.INTEGER) {
        return (Integer)((Short)o).intValue();
      }
      else if(to == Type.LONG) {
        return (Long)((Short)o).longValue();
      }
    }

    if(from == Type.INTEGER
       && to == Type.LONG) {
      return (Long)((Integer)o).longValue();
    }

    throw new UnsupportedOperationException("This should not happen.");
  }
}
