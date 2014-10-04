/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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

package com.datatorrent.lib.io.fs;

import com.datatorrent.api.AttributeMap;
import com.datatorrent.api.AttributeMap.Attribute;
import com.datatorrent.api.Context.OperatorContext;
import javax.annotation.Nonnull;

public class DummyContext implements OperatorContext
{
  int id;
  String applicationPath;
  String applicationId;
  AttributeMap attributes;

  public DummyContext(int id)
  {
    this.id = id;
  }

  public DummyContext(int id, @Nonnull AttributeMap map)
  {
    this.id = id;
    this.attributes = map;
  }

  @Override
  public int getId()
  {
    return id;
  }

  @Override
  public void setCounters(Object stats)
  {
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T getValue(Attribute<T> key)
  {
    T value = attributes.get(key);
    if (value != null) {
      return value;
    }

    return null;
  }

  @Override
  public AttributeMap getAttributes()
  {
    return attributes;
  }
}
