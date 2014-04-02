/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.bucket;

import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Maps;

public class Context
{
  private final Map<String, Object> parameters;

  private Context()
  {
    parameters = Maps.newHashMap();
  }

  public Context(@Nonnull Map<String, Object> parameters)
  {
    this();
    this.parameters.putAll(parameters);
  }

  public void setParameter(String name, Object value){
    parameters.put(name, value);
  }

  @Nullable
  public String getString(String name, String defaultValue)
  {
    String value = (String) parameters.get(name);
    if (value != null) {
      return value;
    }
    return defaultValue;
  }

  @Nullable
  public Integer getInt(String name, Integer defaultValue)
  {
    Integer value = (Integer) parameters.get(name);
    if (value != null) {
      return value;
    }
    return defaultValue;
  }

  @Nullable
  public Long getLong(String name, Long defaultValue)
  {
    Long value = (Long) parameters.get(name);
    if (value != null) {
      return value;
    }
    return defaultValue;
  }

  @Nullable
  public Boolean getBoolean(String name, Boolean defaultValue)
  {
    Boolean value = (Boolean) parameters.get(name);
    if (value != null) {
      return value;
    }
    return defaultValue;
  }

  @Nullable
  public Object getObject(String name, Object defaultValue)
  {
    Object value = parameters.get(name);
    if (value != null) {
      return value;
    }
    return defaultValue;
  }
}
