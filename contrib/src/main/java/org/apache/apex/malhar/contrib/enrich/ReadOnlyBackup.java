/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.contrib.enrich;

import java.util.List;
import java.util.Map;

import org.apache.apex.malhar.lib.util.FieldInfo;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * ReadOnly abstract implementation of BackendLoader.
 *
 * @since 3.4.0
 */
@InterfaceStability.Evolving
public abstract class ReadOnlyBackup implements BackendLoader
{
  protected transient List<FieldInfo> includeFieldInfo;
  protected transient List<FieldInfo> lookupFieldInfo;

  @Override
  public void put(Object key, Object value)
  {
    throw new UnsupportedOperationException("Not supported operation");
  }

  @Override
  public void putAll(Map<Object, Object> m)
  {
    throw new UnsupportedOperationException("Not supported operation");
  }

  @Override
  public void remove(Object key)
  {
    throw new UnsupportedOperationException("Not supported operation");
  }

  @Override
  public void setFieldInfo(List<FieldInfo> lookupFieldInfo, List<FieldInfo> includeFieldInfo)
  {
    this.includeFieldInfo = includeFieldInfo;
    this.lookupFieldInfo = lookupFieldInfo;
  }
}
