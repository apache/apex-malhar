/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.appdata.schemas;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public enum Type
{
  BOOLEAN("boolean", JSONType.BOOLEAN, Boolean.class,
          Collections.unmodifiableSet(new HashSet<Type>())),
  CHAR("char", JSONType.STRING, Character.class,
       Collections.unmodifiableSet(new HashSet<Type>())),
  DOUBLE("double", JSONType.NUMBER, Double.class,
         Collections.unmodifiableSet(new HashSet<Type>())),
  FLOAT("float", JSONType.NUMBER, Float.class,
        ImmutableSet.of(DOUBLE)),
  LONG("long", JSONType.NUMBER, Long.class,
       Collections.unmodifiableSet(new HashSet<Type>())),
  INTEGER("integer", JSONType.NUMBER, Integer.class,
          ImmutableSet.of(LONG)),
  SHORT("short", JSONType.NUMBER, Short.class,
        ImmutableSet.of(INTEGER, LONG)),
  BYTE("byte", JSONType.NUMBER, Byte.class,
       ImmutableSet.of(SHORT, INTEGER, LONG));

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

  Type(String name, JSONType jsonType, Class clazz, Set<Type> higherTypes)
  {
    this.name = name;
    this.jsonType = jsonType;
    this.clazz = clazz;
    this.higherTypes = higherTypes;
  }

  public String getName()
  {
    return name;
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
