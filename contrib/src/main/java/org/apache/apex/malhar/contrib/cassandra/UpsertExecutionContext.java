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
package org.apache.apex.malhar.contrib.cassandra;

import com.datastax.driver.core.ConsistencyLevel;

/**
 * <p>Each mutation in the cassandra table is decided by a context which can be passed to the Operator at runtime.
 * This class represents such a context. The context is to be set by the upstream operator and is used to
 * define how to mutate a row by passing it as a tuple to the {@link AbstractUpsertOutputOperator}
 * The row to be mutated is represented by the payload represented by the template parameter T</p>
 * <p>The following aspects of the mutation can be controlled by the context tuple.<il>
 *  <li>Collection Mutation Style</li>
 *  <li>List Placement style</li>
 *  <li>Null Handling styles for partial mutations of a given row</li>
 *  <li>Update only if a Primary Key exists</li>
 *  <li>Override the TTL that is set at the default connection config.
 *  See {@link ConnectionStateManager.ConnectionBuilder} to set the default TTL for all payload executions</li>
 *  <li>Override the default Consistency level to be used for the current mutation
 *  See {@link ConnectionStateManager.ConnectionBuilder} for setting default consistency.</li>
 * </il></p>
 * @since 3.6.0
 */
public class UpsertExecutionContext<T>
{

  private CollectionMutationStyle collectionMutationStyle = CollectionMutationStyle.UNDEFINED;


  private ListPlacementStyle listPlacementStyle = ListPlacementStyle.UNDEFINED;

  private NullHandlingMutationStyle nullHandlingMutationStyle = NullHandlingMutationStyle.UNDEFINED;

  private int overridingTTL;

  private ConsistencyLevel overridingConsistencyLevel;

  private boolean isTtlOverridden = false;

  private boolean isOverridingConsistencyLevelSet = false;

  private boolean updateOnlyIfPrimaryKeyExists = false;

  private T payload;

  public T getPayload()
  {
    return payload;
  }

  public void setPayload(T payload)
  {
    this.payload = payload;
  }

  /**
   * Represents if the collection object inside the pojo needs to be added or remove
   * from the table column represented by the POJO field. Refer {@link CollectionMutationStyle} for possible value
   * If set as UNDEFINED, the default behaviour is to add the entry to the collection column
   * @return
  */
  public CollectionMutationStyle getCollectionMutationStyle()
  {
    return collectionMutationStyle;
  }

  public void setCollectionMutationStyle(CollectionMutationStyle collectionMutationStyle)
  {
    this.collectionMutationStyle = collectionMutationStyle;
  }

  /**
   * Represents how the POJO List type field is mutated on the cassandra column which is of type
   * list. Note that this is only applicable for list collections and decides whether the POJO element ( of type list)
   * will be placed before
   * @return The List Placement style that is used to execute this mutation. Defaults to Append mode if not set
  */
  public ListPlacementStyle getListPlacementStyle()
  {
    return listPlacementStyle;
  }

  public void setListPlacementStyle(ListPlacementStyle listPlacementStyle)
  {
    this.listPlacementStyle = listPlacementStyle;
  }

  /**
   * This decides how to handle nulls for POJO fields. This comes in handy when one would want to
   * avoid a read of the column and then mutate the row i.e. in other words if the POJO is representing only a subset
   * of the columns that needs to be mutated on the table. In such scenarios, set NullHandlingMutation Style to
   * IGNORE_NULL_COLUMNS which would make the operator avoid an update on the column and thus preventing over-writing
   * an existing value
   * @return The Null handling style that is going to be used for the execution context. Defaults to Set Nulls and not
   * ignore them
   * */
  public NullHandlingMutationStyle getNullHandlingMutationStyle()
  {
    return nullHandlingMutationStyle;
  }

  public void setNullHandlingMutationStyle(NullHandlingMutationStyle nullHandlingMutationStyle)
  {
    this.nullHandlingMutationStyle = nullHandlingMutationStyle;
  }

  /**
   * This decides if we want to override the default TTL if at all set in the
   * {@link org.apache.apex.malhar.contrib.cassandra.ConnectionStateManager.ConnectionBuilder} that is used to execute a
   * mutation. Note that TTLs are not mandatory for mutations.
   * Also it is supported to have TTLs only for the current execution context but not set a default at the
   * connection state manager level
   * Unit of time is seconds
   * @return The overriding TTL that will be used for the given execution context.
     */
  public int getOverridingTTL()
  {
    return overridingTTL;
  }

  public void setOverridingTTL(int overridingTTL)
  {
    this.overridingTTL = overridingTTL;
    isTtlOverridden = true;
  }

  public boolean isTtlOverridden()
  {
    return isTtlOverridden;
  }

  /**
   * Used to override the default consistency level that is set at the {@link ConnectionStateManager } level
   * The default is to use LOCAL_QUORUM. This can be overridden at the execution context level on a per
   * tuple basis.
   * @return The consistency level that would be used to execute the current payload mutation
  * */
  public ConsistencyLevel getOverridingConsistencyLevel()
  {
    return overridingConsistencyLevel;
  }

  public void setOverridingConsistencyLevel(ConsistencyLevel overridingConsistencyLevel)
  {
    this.overridingConsistencyLevel = overridingConsistencyLevel;
    isOverridingConsistencyLevelSet = true;
  }

  public boolean isOverridingConsistencyLevelSet()
  {
    return isOverridingConsistencyLevelSet;
  }

  public boolean isUpdateOnlyIfPrimaryKeyExists()
  {
    return updateOnlyIfPrimaryKeyExists;
  }

  /**
   * Used to execute a mutation only if the primary key exists. This can be used to conditionally execute
   * a mutation i.e. if only the primary key exists. This can be used to force an "UPDATE" only use case
   * for the current mutation.
   * @param updateOnlyIfPrimaryKeyExists
  */
  public void setUpdateOnlyIfPrimaryKeyExists(boolean updateOnlyIfPrimaryKeyExists)
  {
    this.updateOnlyIfPrimaryKeyExists = updateOnlyIfPrimaryKeyExists;
  }

  enum CollectionMutationStyle implements BaseMutationStyle
  {
    ADD_TO_EXISTING_COLLECTION,
    REMOVE_FROM_EXISTING_COLLECTION,
    UNDEFINED
  }

  enum ListPlacementStyle implements BaseMutationStyle
  {
    APPEND_TO_EXISTING_LIST,
    PREPEND_TO_EXISTING_LIST,
    UNDEFINED
  }

  enum NullHandlingMutationStyle implements BaseMutationStyle
  {
    IGNORE_NULL_COLUMNS,
    SET_NULL_COLUMNS,
    UNDEFINED
  }

  interface BaseMutationStyle
  {

  }

}
