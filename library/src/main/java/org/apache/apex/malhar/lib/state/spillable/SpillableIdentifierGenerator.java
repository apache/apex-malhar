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
package org.apache.apex.malhar.lib.state.spillable;

import org.apache.hadoop.classification.InterfaceStability;

/**
 * Classes implementing this interface can be used as generators for identifiers for Spillable data structures. This is
 * mainly used in implementations of {@link SpillableComplexComponent}.
 *
 * @since 3.5.0
 */
@InterfaceStability.Evolving
public interface SpillableIdentifierGenerator
{
  /**
   * Generators the next valid identifier for a Spillable data structure.
   * @return A byte array which represents the next valid identifier for a Spillable data structure.
   */
  byte[] next();

  /**
   * Registers the given identifier with this {@link SpillableIdentifierGenerator}.
   * @param identifier The identifier to register with this {@link SpillableIdentifierGenerator}.
   */
  void register(byte[] identifier);
}
