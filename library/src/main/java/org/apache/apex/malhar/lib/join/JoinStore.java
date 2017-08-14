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
package org.apache.apex.malhar.lib.join;

import java.util.List;

import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.Component;

/**
 * <p>
 * Interface of store for join operation.
 * </p>
 *
 * @since 3.4.0
 */
@InterfaceStability.Unstable
public interface JoinStore extends Component
{
  /**
   * Generate the store
   */

  /**
   * Perform the committed operation
   * @param windowId
   */
  void committed(long windowId);

  /**
   * Save the state of store
   * @param windowId
   */
  void checkpointed(long windowId);

  /**
   * Add the operations, any needed for store before begin the window
   * @param windowId
   */
  void beginWindow(long windowId);

  /**
   *
   */
  void endWindow();

  /**
   * Get the key from the given tuple and with that key, get the tuples which satisfies the join constraint
   * from the store.
   *
   * @param tuple Given tuple
   * @return the valid tuples which statisfies the join constraint
   */
  List<?> getValidTuples(Object tuple);

  /**
   * Insert the given tuple
   *
   * @param tuple Given tuple
   */
  boolean put(Object tuple);

  /**
   * Return the unmatched events from store
   *
   * @return the unmatched events
   */
  List<?> getUnMatchedTuples();

  /**
   * Set if the join type is outer
   *
   * @param isOuter Specifies the join type is outer join or not
   */
  void isOuterJoin(boolean isOuter);
}
