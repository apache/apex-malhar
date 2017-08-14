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

import org.apache.apex.malhar.lib.db.cache.CacheManager;
import org.apache.apex.malhar.lib.util.FieldInfo;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Interface for store to be used in enrichment
 *
 * @since 3.4.0
 */
@InterfaceStability.Evolving
public interface BackendLoader extends CacheManager.Backup
{
  /**
   * Set {@link FieldInfo} for lookup fields and also include fields.
   * Calling this method is mandatory for correct functioning of backend loader.
   *
   * @param lookupFieldInfo  List of {@link FieldInfo} that will be used as key in lookup.
   * @param includeFieldInfo List of {@link FieldInfo} that will be retrieved from store.
   */
  void setFieldInfo(List<FieldInfo> lookupFieldInfo, List<FieldInfo> includeFieldInfo);
}
