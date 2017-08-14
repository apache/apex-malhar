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


import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

/**
 * Used to generate CQL strings that can be used to generate prepared statements.
 *
 * @since 3.6.0
 */
public class CassandraPreparedStatementGenerator
{

  private Set<String> pkColumnNames;

  private Set<String> counterColumns;

  private Set<String> listColumns;

  private Set<String> mapColumns;

  private Set<String> setColumns;

  private Map<String, DataType> columnDefinitions;

  private static final transient Logger LOG = LoggerFactory.getLogger(CassandraPreparedStatementGenerator.class);

  public static final String TTL_PARAM_NAME = "ttl";

  public CassandraPreparedStatementGenerator(Set<String> pkColumnNames, Set<String> counterColumns,
      Set<String> listColumns, Set<String> mapColumns, Set<String> setColumns,
      Map<String, DataType> columnDefinitions)
  {
    this.pkColumnNames = pkColumnNames;
    this.counterColumns = counterColumns;
    this.listColumns = listColumns;
    this.mapColumns = mapColumns;
    this.setColumns = setColumns;
    this.columnDefinitions = columnDefinitions;
  }

  public void generatePreparedStatements(Session session,Map<Long, PreparedStatement> preparedStatementTypes,
      String keyspaceName,String tableName)
  {

    Map<Long, String> stringsWithoutPKAndExistsClauses = generatePreparedStatementsQueryStrings(keyspaceName,tableName);
    String ifExistsClause = " IF EXISTS";
    Map<Long, String> finalSetOfQueryStrings = new HashMap<>();
    for (Long currentIndexPos : stringsWithoutPKAndExistsClauses.keySet()) {
      StringBuilder aQueryStub = new StringBuilder(stringsWithoutPKAndExistsClauses.get(currentIndexPos));
      buildWhereClauseForPrimaryKeys(aQueryStub);
      finalSetOfQueryStrings.put(currentIndexPos +
          getSlotIndexForMutationContextPreparedStatement(EnumSet.of(
          AbstractUpsertOutputOperator.OperationContext.IF_EXISTS_CHECK_ABSENT)),
          aQueryStub.toString());
      if (counterColumns.size() == 0) {
        // IF exists cannot be used for counter column tables
        finalSetOfQueryStrings.put(currentIndexPos +
            getSlotIndexForMutationContextPreparedStatement(EnumSet.of(
            AbstractUpsertOutputOperator.OperationContext.IF_EXISTS_CHECK_PRESENT)),
            aQueryStub.toString() + ifExistsClause);
      }
    }
    for (Long currentIndexPos : finalSetOfQueryStrings.keySet()) {
      String currentQueryStr = finalSetOfQueryStrings.get(currentIndexPos);
      LOG.info("Registering query support for " + currentQueryStr);
      PreparedStatement preparedStatementForThisQuery = session.prepare(currentQueryStr);
      preparedStatementTypes.put(currentIndexPos, preparedStatementForThisQuery);
    }
  }

  private void buildWhereClauseForPrimaryKeys(final StringBuilder queryExpression)
  {
    queryExpression.append(" WHERE ");
    int count = 0;
    for (String pkColName : pkColumnNames) {
      if (count > 0) {
        queryExpression.append(" AND ");
      }
      count += 1;
      queryExpression.append(" ").append(pkColName).append(" = :").append(pkColName);
    }
  }


  private void buildQueryStringForTTLSetCollectionsAppendAndListPrepend(StringBuilder updateQueryRoot,
      String ttlSetString, Map<Long,String> queryStrings)
  {
    // TTL set , Collections Append , List prepend
    StringBuilder queryExpTTLSetCollAppendListPrepend = new StringBuilder(updateQueryRoot.toString());
    queryExpTTLSetCollAppendListPrepend.append(ttlSetString);
    buildNonPKColumnsExpression(queryExpTTLSetCollAppendListPrepend,
        UpsertExecutionContext.ListPlacementStyle.PREPEND_TO_EXISTING_LIST,
        UpsertExecutionContext.CollectionMutationStyle.ADD_TO_EXISTING_COLLECTION);
    queryStrings.put(getSlotIndexForMutationContextPreparedStatement(EnumSet.of(
        AbstractUpsertOutputOperator.OperationContext.TTL_SET,
        AbstractUpsertOutputOperator.OperationContext.COLLECTIONS_APPEND,
        AbstractUpsertOutputOperator.OperationContext.LIST_PREPEND
    )), queryExpTTLSetCollAppendListPrepend.toString());
  }

  private void buildQueryStringForTTLSetCollectionsAppendAndListAppend(StringBuilder updateQueryRoot,
      String ttlSetString,Map<Long,String> queryStrings)
  {
    // TTL set , Collections Append , List append
    StringBuilder queryExpTTLSetCollAppendListAppend = new StringBuilder(updateQueryRoot.toString());
    queryExpTTLSetCollAppendListAppend.append(ttlSetString);
    buildNonPKColumnsExpression(queryExpTTLSetCollAppendListAppend,
        UpsertExecutionContext.ListPlacementStyle.APPEND_TO_EXISTING_LIST,
        UpsertExecutionContext.CollectionMutationStyle.ADD_TO_EXISTING_COLLECTION);
    queryStrings.put(getSlotIndexForMutationContextPreparedStatement(EnumSet.of(
        AbstractUpsertOutputOperator.OperationContext.TTL_SET,
        AbstractUpsertOutputOperator.OperationContext.COLLECTIONS_APPEND,
        AbstractUpsertOutputOperator.OperationContext.LIST_APPEND
    )), queryExpTTLSetCollAppendListAppend.toString());
  }

  private void buildQueryStringForTTLSetCollectionsRemove(StringBuilder updateQueryRoot,
      String ttlSetString,Map<Long,String> queryStrings)
  {
    // TTL set , Collections Remove
    StringBuilder queryExpTTLSetCollRemove = new StringBuilder(updateQueryRoot.toString());
    queryExpTTLSetCollRemove.append(ttlSetString);
    buildNonPKColumnsExpression(queryExpTTLSetCollRemove,
        UpsertExecutionContext.ListPlacementStyle.APPEND_TO_EXISTING_LIST, // Just in case user sets it
        UpsertExecutionContext.CollectionMutationStyle.REMOVE_FROM_EXISTING_COLLECTION);
    queryStrings.put(getSlotIndexForMutationContextPreparedStatement(EnumSet.of(
        AbstractUpsertOutputOperator.OperationContext.TTL_SET,
        AbstractUpsertOutputOperator.OperationContext.COLLECTIONS_REMOVE,
        AbstractUpsertOutputOperator.OperationContext.LIST_APPEND
    )), queryExpTTLSetCollRemove.toString());
  }

  private void buildQueryStringForTTLNotSetCollectionsAppendWithListPrepend(StringBuilder updateQueryRoot,
      Map<Long,String> queryStrings)
  {
    // TTL Not set , Collections Append , List prepend
    StringBuilder queryExpTTLNotSetCollAppendListPrepend = new StringBuilder(updateQueryRoot.toString());
    queryExpTTLNotSetCollAppendListPrepend.append(" SET ");
    buildNonPKColumnsExpression(queryExpTTLNotSetCollAppendListPrepend,
        UpsertExecutionContext.ListPlacementStyle.PREPEND_TO_EXISTING_LIST,
        UpsertExecutionContext.CollectionMutationStyle.ADD_TO_EXISTING_COLLECTION);
    queryStrings.put(getSlotIndexForMutationContextPreparedStatement(EnumSet.of(
        AbstractUpsertOutputOperator.OperationContext.TTL_NOT_SET,
        AbstractUpsertOutputOperator.OperationContext.COLLECTIONS_APPEND,
        AbstractUpsertOutputOperator.OperationContext.LIST_PREPEND
    )), queryExpTTLNotSetCollAppendListPrepend.toString());
  }

  private void buildQueryStringForTTLNotSetCollectionsAppendWithListAppend(StringBuilder updateQueryRoot,
      Map<Long,String> queryStrings)
  {
    // TTL Not set , Collections Append , List append
    StringBuilder queryExpTTLNotSetCollAppendListAppend = new StringBuilder(updateQueryRoot.toString());
    queryExpTTLNotSetCollAppendListAppend.append(" SET ");
    buildNonPKColumnsExpression(queryExpTTLNotSetCollAppendListAppend,
        UpsertExecutionContext.ListPlacementStyle.APPEND_TO_EXISTING_LIST,
        UpsertExecutionContext.CollectionMutationStyle.ADD_TO_EXISTING_COLLECTION);
    queryStrings.put(getSlotIndexForMutationContextPreparedStatement(EnumSet.of(
        AbstractUpsertOutputOperator.OperationContext.TTL_NOT_SET,
        AbstractUpsertOutputOperator.OperationContext.COLLECTIONS_APPEND,
        AbstractUpsertOutputOperator.OperationContext.LIST_APPEND
    )), queryExpTTLNotSetCollAppendListAppend.toString());
  }


  private void buildQueryStringForTTLNotSetCollectionsRemove(StringBuilder updateQueryRoot,
      Map<Long,String> queryStrings)
  {
    // TTL Not set , Collections Remove
    StringBuilder queryExpTTLNotSetCollRemove = new StringBuilder(updateQueryRoot.toString());
    queryExpTTLNotSetCollRemove.append(" SET ");
    buildNonPKColumnsExpression(queryExpTTLNotSetCollRemove,
        UpsertExecutionContext.ListPlacementStyle.APPEND_TO_EXISTING_LIST, // Just in case user sets it
        UpsertExecutionContext.CollectionMutationStyle.REMOVE_FROM_EXISTING_COLLECTION);
    queryStrings.put(getSlotIndexForMutationContextPreparedStatement(EnumSet.of(
        AbstractUpsertOutputOperator.OperationContext.TTL_NOT_SET,
        AbstractUpsertOutputOperator.OperationContext.COLLECTIONS_REMOVE,
        AbstractUpsertOutputOperator.OperationContext.LIST_APPEND
    )), queryExpTTLNotSetCollRemove.toString());
  }

  private Map<Long, String> generatePreparedStatementsQueryStrings(String keyspaceName,String tableName)
  {
    Map<Long, String> queryStrings = new HashMap<>();
    //UPDATE keyspace_name.table_name USING option AND option SET assignment, assignment, ... WHERE row_specification
    StringBuilder updateQueryRoot = new StringBuilder(" UPDATE " + keyspaceName +
        "." + tableName + " ");
    String ttlSetString = " USING ttl :" + TTL_PARAM_NAME + " SET ";
    buildQueryStringForTTLSetCollectionsAppendAndListPrepend(updateQueryRoot,ttlSetString,queryStrings);
    buildQueryStringForTTLSetCollectionsAppendAndListAppend(updateQueryRoot,ttlSetString,queryStrings);
    buildQueryStringForTTLSetCollectionsRemove(updateQueryRoot,ttlSetString,queryStrings);
    buildQueryStringForTTLNotSetCollectionsAppendWithListPrepend(updateQueryRoot,queryStrings);
    buildQueryStringForTTLNotSetCollectionsAppendWithListAppend(updateQueryRoot,queryStrings);
    buildQueryStringForTTLNotSetCollectionsRemove(updateQueryRoot,queryStrings);
    return queryStrings;
  }


  public static long getSlotIndexForMutationContextPreparedStatement(
      final EnumSet<AbstractUpsertOutputOperator.OperationContext> context)
  {
    Iterator<AbstractUpsertOutputOperator.OperationContext> itrForContexts = context.iterator();
    long indexValue = 0;
    while (itrForContexts.hasNext()) {
      AbstractUpsertOutputOperator.OperationContext aContext = itrForContexts.next();
      indexValue += Math.pow(10, aContext.ordinal());
    }
    return indexValue;
  }

  private void buildNonPKColumnsExpression(final StringBuilder queryExpression,
      UpsertExecutionContext.ListPlacementStyle listPlacementStyle,
      UpsertExecutionContext.CollectionMutationStyle collectionMutationStyle)
  {
    int count = 0;
    for (String colNameEntry : columnDefinitions.keySet()) {
      if (pkColumnNames.contains(colNameEntry)) {
        continue;
      }
      if (count > 0) {
        queryExpression.append(",");
      }
      count += 1;
      if (counterColumns.contains(colNameEntry)) {
        queryExpression.append(" " + colNameEntry + " = " + colNameEntry + " + :" + colNameEntry);
        continue;
      }
      DataType dataType = columnDefinitions.get(colNameEntry);
      if ((!dataType.isCollection()) && (!counterColumns.contains(colNameEntry))) {
        queryExpression.append(" " + colNameEntry + " = :" + colNameEntry);
        continue;
      }
      if ((dataType.isCollection()) && (!dataType.isFrozen())) {
        if (collectionMutationStyle == UpsertExecutionContext.CollectionMutationStyle.REMOVE_FROM_EXISTING_COLLECTION) {
          queryExpression.append(" " + colNameEntry + " = " + colNameEntry + " - :" + colNameEntry);
        }
        if (collectionMutationStyle == UpsertExecutionContext.CollectionMutationStyle.ADD_TO_EXISTING_COLLECTION) {
          if ((setColumns.contains(colNameEntry)) || (mapColumns.contains(colNameEntry))) {
            queryExpression.append(" " + colNameEntry + " = " + colNameEntry + " + :" + colNameEntry);
          }
          if ((listColumns.contains(colNameEntry)) &&
              (listPlacementStyle == UpsertExecutionContext.ListPlacementStyle.APPEND_TO_EXISTING_LIST)) {
            queryExpression.append(" " + colNameEntry + " = " + colNameEntry + " + :" + colNameEntry);
          }
          if ((listColumns.contains(colNameEntry)) &&
              (listPlacementStyle == UpsertExecutionContext.ListPlacementStyle.PREPEND_TO_EXISTING_LIST)) {
            queryExpression.append(" " + colNameEntry + " = :" + colNameEntry + " + " + colNameEntry);
          }
        }
      } else {
        if ((dataType.isCollection()) && (dataType.isFrozen())) {
          queryExpression.append(" " + colNameEntry + " = :" + colNameEntry);
        }
      }
    }
  }

}

