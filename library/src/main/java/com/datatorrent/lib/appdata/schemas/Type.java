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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public enum Type
{
  BOOLEAN("boolean", 1, JSONType.BOOLEAN, Boolean.class,
          Collections.unmodifiableSet(new HashSet<Type>())),
  STRING("string", -1, JSONType.STRING, String.class,
         Collections.unmodifiableSet(new HashSet<Type>())),
  CHAR("char", 2, JSONType.STRING, Character.class,
       ImmutableSet.of(STRING)),
  DOUBLE("double", 8, JSONType.NUMBER, Double.class,
         Collections.unmodifiableSet(new HashSet<Type>())),
  FLOAT("float", 4, JSONType.NUMBER, Float.class,
        ImmutableSet.of(DOUBLE)),
  LONG("long", 8, JSONType.NUMBER, Long.class,
       Collections.unmodifiableSet(new HashSet<Type>())),
  INTEGER("integer", 4, JSONType.NUMBER, Integer.class,
          ImmutableSet.of(LONG)),
  SHORT("short", 2, JSONType.NUMBER, Short.class,
        ImmutableSet.of(INTEGER, LONG)),
  BYTE("byte", 1, JSONType.NUMBER, Byte.class,
       ImmutableSet.of(SHORT, INTEGER, LONG));

  public static final Set<Type> NUMERIC_TYPES = ImmutableSet.of(BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE);
  public static final Set<Type> NON_NUMERIC_TYPES = ImmutableSet.of(BOOLEAN, CHAR, STRING);
  public static final Map<String, Type> NAME_TO_TYPE;
  public static final Map<Class, Type> CLASS_TO_TYPE;

  static {
    Map<String, Type> nameToType = Maps.newHashMap();

    for(Type type: Type.values()) {
      nameToType.put(type.getName(), type);
    }

    NAME_TO_TYPE = Collections.unmodifiableMap(nameToType);

    Map<Class, Type> clazzToType = Maps.newHashMap();

    for(Type type: Type.values()) {
      clazzToType.put(type.getClazz(), type);
    }

    CLASS_TO_TYPE = Collections.unmodifiableMap(clazzToType);
  }

  private final String name;
  private final JSONType jsonType;
  private final Class clazz;
  private final Set<Type> higherTypes;
  private final int byteSize;

  Type(String name,
       int byteSize,
       JSONType jsonType,
       Class clazz,
       Set<Type> higherTypes)
  {
    this.name = name;
    this.byteSize = byteSize;
    this.jsonType = jsonType;
    this.clazz = clazz;
    this.higherTypes = higherTypes;
  }

  public String getName()
  {
    return name;
  }

  public int getByteSize()
  {
    return byteSize;
  }

  public JSONType getJSONType()
  {
    return jsonType;
  }

  public Class getClazz()
  {
    return clazz;
  }

  public Set<Type> getHigherTypes()
  {
    return higherTypes;
  }

  public boolean isChildOf(Type type)
  {
    return higherTypes.contains(type);
  }

  public static boolean areRelated(Type a, Type b)
  {
    return a == b || a.isChildOf(b) || b.isChildOf(a);
  }

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
