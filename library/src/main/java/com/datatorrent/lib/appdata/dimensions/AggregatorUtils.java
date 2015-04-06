/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.dimensions;

import com.datatorrent.lib.appdata.schemas.Type;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.Map;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public final class AggregatorUtils
{
  public static final Map<Type, Type> IDENTITY_TYPE_MAP;
  public static final Map<Type, Type> IDENTITY_NUMBER_TYPE_MAP;

  static {
    Map<Type, Type> identityTypeMap = Maps.newHashMap();

    for(Type type: Type.values()) {
      identityTypeMap.put(type, type);
    }

    IDENTITY_TYPE_MAP = Collections.unmodifiableMap(identityTypeMap);

    Map<Type, Type> identityNumberTypeMap = Maps.newHashMap();

    for(Type type: Type.NUMERIC_TYPES) {
      identityNumberTypeMap.put(type, type);
    }

    IDENTITY_NUMBER_TYPE_MAP = Collections.unmodifiableMap(identityNumberTypeMap);
  }

  private AggregatorUtils()
  {
    //Don't instantiate this class.
  }
}
