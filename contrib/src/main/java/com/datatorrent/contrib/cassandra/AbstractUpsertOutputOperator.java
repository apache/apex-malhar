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
package com.datatorrent.contrib.cassandra;

import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.wal.FSWindowDataManager;
import org.apache.apex.malhar.lib.wal.WindowDataManager;
import org.apache.hadoop.classification.InterfaceStability;

import com.codahale.metrics.Timer;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.Metrics;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TypeCodec;



import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.util.PojoUtils;


import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An abstract operator that is used to mutate cassandra rows using PreparedStatements for faster executions
 * and accommodates EXACTLY_ONCE Semantics if concrete implementations choose to implement an abstract method with
 * meaningful implementation (as Cassandra is not a pure transactional database , the burden is on the concrete
 * implementation of the operator ONLY during the reconciliation window (and not for any other windows).
 *
 * The typical execution flow is as follows :
 *  1. Create a concrete implementation of this class by extending this class and implement a few methods.
 *  2. Define the payload that is the POJO that represents a Cassandra Row is part of this execution context
 *     {@link UpsertExecutionContext}. The payload is a template Parameter of this class
 *  3. The Upstream operator that wants to write to Cassandra does the following
 *      a. Create an instance of {@link UpsertExecutionContext}
 *      b. Set the payload ( an instance of the POJO created as step two above )
 *      c. Set additional execution context parameters like CollectionHandling style, List placement Styles
 *         overriding TTLs, Update only if Primary keys exist and Consistency Levels etc.
 *  4. The concrete implementation would then execute this context as a cassandra row mutation
 *
 * This operator supports the following features
 * 1. Highly customizable Connection policies. This is achieved by specifying the ConnectionStateManager.
 *    There are a good number of connection management aspects that can be
 *    controlled via {@link ConnectionStateManager} like consistency, load balancing, connection retries,
 *    table to use, keyspace to use etc. Please refer javadoc of {@link ConnectionStateManager}
 * 2. Support for Collections : Map, List and Sets are supported
 *    User Defined types as part of collections is also supported.
 * 3. Support exists for both adding to an existing collection or removing entries from an existing collection.
 *    The POJO field that represents a collection is used to represent the collection that is added or removed.
 *    Thus this can be used to avoid a pattern of read and then write the final value into the cassandra column
 *    which can be used for low latency / high write pattern applications as we can avoid a read in the process.
 * 4. Supports List Placements : The execution context can be used to specify where the new incoming list
 *    is to be added ( in case there is an existing list in the current column of the current row being mutated.
 *    Supported options are APPEND or PREPEND to an existing list
 * 5. Support for User Defined Types. A pojo can have fields that represent the Cassandra Columns that are custom
 *    user defined types. Concrete implementations of the operator provide a mapping of the cassandra column name
 *    to the TypeCodec that is to be used for that field inside cassandra. Please refer javadocs of
 *    {@link this.getCodecsForUserDefinedTypes() } for more details
 * 6. Support for custom mapping of POJO payload field names to that of cassandra columns. Practically speaking,
 *    POJO field names might not always match with Cassandra Column names and hence this support. This will also avoid
 *    writing a POJO just for the cassandra operator and thus an existing POJO can be passed around to this operator.
 *    Please refer javadoc {@link this.getPojoFieldNameToCassandraColumnNameOverride()} for an example
 * 7. TTL support - A default TTL can be set for the Connection ( via {@link ConnectionStateManager} and then used
 *    for all mutations. This TTL can further be overridden at a tuple execution level to accomodate use cases of
 *    setting custom column expirations typically useful in wide row implementations.
 * 8. Support for Counter Column tables. Counter tables are also supported with the values inside the incoming
 *    POJO added/subtracted from the counter column accordingly. Please note that the value is not absolute set but
 *    rather representing the value that needs to be added to or subtracted from the current counter.
 * 9. Support for Composite Primary Keys is also supported. All the POJO fields that map to the composite
 *    primary key are used to resolve the primary key in case of a Composite Primary key table
 * 10. Support for conditional updates : This operator can be used as an Update Only operator as opposed to an
 *     Upsert operator. i.e. Update only IF EXISTS . This is achieved by setting the appropriate boolean in the
 *     {@link UpsertExecutionContext} tuple that is passed from the upstream operator.
 * 11. Lenient mapping of POJO fields to Cassandra column names. By default the POJO field names are case insensitive
 *     to cassandra column names. This can be further enhanced by over-riding mappings. Please refer feature 6 above.
 * 12. Defaults can be overridden at at tuple execution level for TTL & Consistency Policies
 * 13. Support for handling Nulls i.e. whether null values in the POJO are to be persisted as is or to be ignored so
 *     that the application need not perform a read to populate a POJO field if it is not available in the context
 * 14. A few autometrics are provided for monitoring the latency aspects of the cassandra cluster
 */

@InterfaceStability.Evolving
public abstract class AbstractUpsertOutputOperator extends BaseOperator implements
    Operator.ActivationListener<Context.OperatorContext>, Operator.CheckpointNotificationListener
{

  public static final String TTL_PARAM_NAME = "ttl";

  ConnectionStateManager connectionStateManager;

  transient WindowDataManager windowDataManager;

  private long currentWindowId;

  private transient boolean isInSafeMode;

  private transient long reconcilingWindowId;

  private transient boolean isInReconcilingMode;

  transient Session session;

  transient Cluster cluster;

  transient String keyspaceName;

  transient String tableName;

  transient Map<String, TypeCodec> complexTypeCodecs;

  transient Map<String, Class> userDefinedTypesClass;

  transient Map<String, DataType> columnDefinitions;

  transient Map<String, String> colNamesMap;

  transient Map<DataType.Name, List<String>> dataTypeGroupedColumns;

  transient Set<String> pkColumnNames;

  transient Set<String> counterColumns;

  transient Set<String> collectionColumns;

  transient Set<String> listColumns;

  transient Set<String> mapColumns;

  transient Set<String> setColumns;

  transient Set<String> userDefinedTypeColumns;

  transient Set<String> regularColumns;

  transient Map<String, String> pojoFieldNamesToCassandraColumns;

  Map<String, Object> getters;

  Map<String, TypeCodec> codecsForCassandraColumnNames;

  transient Map<Long, PreparedStatement> preparedStatementTypes;
  transient Class<?> tuplePayloadClass;

  private transient Logger LOG = LoggerFactory.getLogger(AbstractUpsertOutputOperator.class);

  // Metrics

  @AutoMetric
  transient long ignoredRequestsDuetoIfExistsCheck = 0;

  @AutoMetric
  transient long successfullWrites = 0;

  @AutoMetric
  long totalWriteRequestsAttempted = 0;

  @AutoMetric
  transient int numberOfHostsWrittenTo = 0;

  @AutoMetric
  transient double fifteenMinuteWriteRateLatency = 0.0;

  @AutoMetric
  transient double fiveMinuteWriteRateLatency = 0.0;

  @AutoMetric
  transient double oneMinuteWriteRateLatency = 0.0;

  @AutoMetric
  transient double meanWriteRateLatency = 0.0;

  @AutoMetric
  transient long totalIgnoresInThisWindow = 0;

  @AutoMetric
  long totalIgnoresSinceStart = 0;

  @AutoMetric
  transient long totalWriteTimeoutsInThisWindow = 0;

  @AutoMetric
  long totalWriteTimeoutsSinceStart = 0;

  @AutoMetric
  transient long totalWriteRetriesInThisWindow =  0;

  @AutoMetric
  long totalWriteRetriesSinceStart = 0;

  @AutoMetric
  transient long writesWithConsistencyOne = 0;

  @AutoMetric
  transient long writesWithConsistencyTwo = 0;

  @AutoMetric
  transient long writesWithConsistencyThree = 0;

  @AutoMetric
  transient long writesWithConsistencyAll = 0;

  @AutoMetric
  transient long writesWithConsistencyLocalOne = 0;

  @AutoMetric
  transient long writesWithConsistencyQuorum = 0;

  @AutoMetric
  transient long writesWithConsistencyLocalQuorum = 0;

  @AutoMetric
  transient long writeWithConsistencyLocalSerial = 0;

  @AutoMetric
  transient long writesWithConsistencyEachQuorum = 0;

  @AutoMetric
  transient long writesWithConsistencySerial = 0;

  @AutoMetric
  transient long writesWithConsistencyAny = 0;

  transient Set<Host> uniqueHostsWrittenToInCurrentWindow;


  @InputPortFieldAnnotation
  public final transient DefaultInputPort<UpsertExecutionContext> input = new DefaultInputPort<UpsertExecutionContext>()
  {
    @Override
    public void process(final UpsertExecutionContext tuple)
    {
      if (!isEligbleForPassivation(tuple)) {
        return;
      }
      BoundStatement stmnt = setDefaultsAndPrepareBoundStatement(tuple);
      ResultSet result = session.execute(stmnt);
      updatePerRowMetric(result);
    }
  }; // end of input port implementation

  protected boolean isEligbleForPassivation(final UpsertExecutionContext tuple)
  {
    if (isInSafeMode) {
      return false;
    }
    if (isInReconcilingMode) {
      return reconcileRecord(tuple,currentWindowId);
    }
    return true;
  }

  private BoundStatement setDefaultsAndPrepareBoundStatement(UpsertExecutionContext tuple)
  {
    UpsertExecutionContext.NullHandlingMutationStyle nullHandlingMutationStyle = tuple.getNullHandlingMutationStyle();
    if (UpsertExecutionContext.NullHandlingMutationStyle.UNDEFINED == nullHandlingMutationStyle) {
      nullHandlingMutationStyle = UpsertExecutionContext.NullHandlingMutationStyle.SET_NULL_COLUMNS;
    }
    boolean setNulls = true;
    if (nullHandlingMutationStyle != UpsertExecutionContext.NullHandlingMutationStyle.SET_NULL_COLUMNS) {
      setNulls = false;
    }
    UpsertExecutionContext.CollectionMutationStyle collectionMutationStyle = tuple.getCollectionMutationStyle();
    if (collectionMutationStyle != null) {
      if (collectionMutationStyle == UpsertExecutionContext.CollectionMutationStyle.UNDEFINED) {
        // done to simplify the prepared statement resolution. Will work fine for tables with no collections as well
        tuple.setCollectionMutationStyle(UpsertExecutionContext.CollectionMutationStyle.ADD_TO_EXISTING_COLLECTION);
      }
    } else {
      tuple.setCollectionMutationStyle(UpsertExecutionContext.CollectionMutationStyle.ADD_TO_EXISTING_COLLECTION);
    }
    UpsertExecutionContext.ListPlacementStyle listPlacementStyle = tuple.getListPlacementStyle();
    if (listPlacementStyle != null) {
      if (listPlacementStyle == UpsertExecutionContext.ListPlacementStyle.UNDEFINED) {
        // done to simplify prepared statement resolution. Works fine for No List column tables as well
        tuple.setListPlacementStyle(UpsertExecutionContext.ListPlacementStyle.APPEND_TO_EXISTING_LIST);
      }
    } else {
      tuple.setListPlacementStyle(UpsertExecutionContext.ListPlacementStyle.APPEND_TO_EXISTING_LIST);
    }
    PreparedStatement preparedStatement = resolvePreparedStatementForCurrentExecutionContext(tuple);
    BoundStatement stmnt = processPayloadForExecution(preparedStatement, tuple, setNulls);
    if ((tuple.isTtlOverridden()) || (connectionStateManager.isTTLSet())) {
      int ttlToUse = connectionStateManager.getDefaultTtlInSecs();
      if (tuple.isTtlOverridden()) {
        ttlToUse = tuple.getOverridingTTL();
      }
      stmnt.setInt(TTL_PARAM_NAME, ttlToUse);
    }
    if (tuple.isOverridingConsistencyLevelSet()) {
      ConsistencyLevel currentConsistencyLevel = tuple.getOverridingConsistencyLevel();
      if (currentConsistencyLevel.isSerial()) {
        stmnt.setSerialConsistencyLevel(tuple.getOverridingConsistencyLevel());
      } else {
        stmnt.setConsistencyLevel(tuple.getOverridingConsistencyLevel());
      }
    }
    LOG.debug("Executing statement " + preparedStatement.getQueryString());
    return stmnt;
  }

  private void updatePerRowMetric(ResultSet result)
  {
    uniqueHostsWrittenToInCurrentWindow.add(result.getExecutionInfo().getQueriedHost());
    updateConsistencyLevelMetrics(result.getExecutionInfo().getAchievedConsistencyLevel());
    successfullWrites += 1;
    if (!result.wasApplied()) {
      ignoredRequestsDuetoIfExistsCheck += 1;
    }
  }

  private void updateConsistencyLevelMetrics(ConsistencyLevel resultConsistencyLevel)
  {
    if (resultConsistencyLevel == null) {
      return;
    }
    switch (resultConsistencyLevel) {
      case ALL:
        writesWithConsistencyAll += 1;
        break;
      case ANY:
        writesWithConsistencyAny += 1;
        break;
      case EACH_QUORUM:
        writesWithConsistencyEachQuorum += 1;
        break;
      case LOCAL_ONE:
        writesWithConsistencyLocalOne += 1;
        break;
      case LOCAL_QUORUM:
        writesWithConsistencyLocalQuorum += 1;
        break;
      case LOCAL_SERIAL:
        writeWithConsistencyLocalSerial += 1;
        break;
      case ONE:
        writesWithConsistencyOne += 1;
        break;
      case QUORUM:
        writesWithConsistencyQuorum += 1;
        break;
      case SERIAL:
        writesWithConsistencySerial += 1;
        break;
      case THREE:
        writesWithConsistencyThree += 1;
        break;
      case TWO:
        writesWithConsistencyTwo += 1;
        break;
      default:
        break;
    }
  }

  /**
   * Shortlists the prepared statement from a cache that is populated initially.
   * @param tuple The execution context that is used to mutate the current cassandra row
   * @return The prepared statement that is applicable for the current execution context
   */
  private PreparedStatement resolvePreparedStatementForCurrentExecutionContext(UpsertExecutionContext tuple)
  {
    EnumSet<OperationContext> operationContextValue = EnumSet.noneOf(OperationContext.class);

    UpsertExecutionContext.CollectionMutationStyle collectionMutationStyle = tuple.getCollectionMutationStyle();
    if (collectionMutationStyle != null) {
      if (collectionMutationStyle == UpsertExecutionContext.CollectionMutationStyle.ADD_TO_EXISTING_COLLECTION) {
        operationContextValue.add(OperationContext.COLLECTIONS_APPEND);
      }
      if (collectionMutationStyle == UpsertExecutionContext.CollectionMutationStyle.REMOVE_FROM_EXISTING_COLLECTION) {
        operationContextValue.add(OperationContext.COLLECTIONS_REMOVE);
      }
    }
    UpsertExecutionContext.ListPlacementStyle listPlacementStyle = tuple.getListPlacementStyle();
    boolean isListContextSet = false;
    if ((listPlacementStyle != null) && (collectionMutationStyle ==
        UpsertExecutionContext.CollectionMutationStyle.ADD_TO_EXISTING_COLLECTION)) {
      if (listPlacementStyle == UpsertExecutionContext.ListPlacementStyle.APPEND_TO_EXISTING_LIST) {
        operationContextValue.add(OperationContext.LIST_APPEND);
        isListContextSet = true;
      }
      if (listPlacementStyle == UpsertExecutionContext.ListPlacementStyle.PREPEND_TO_EXISTING_LIST) {
        operationContextValue.add(OperationContext.LIST_PREPEND);
        isListContextSet = true;
      }
    }
    if (!isListContextSet) {
      // use cases when remove is specified but we do not want to build separate prepared statments
      operationContextValue.add(OperationContext.LIST_APPEND);
    }
    if ((connectionStateManager.isTTLSet()) || (tuple.isTtlOverridden())) {
      operationContextValue.add(OperationContext.TTL_SET);
    } else {
      operationContextValue.add(OperationContext.TTL_NOT_SET);
    }
    if (tuple.isUpdateOnlyIfPrimaryKeyExists()) {
      operationContextValue.add(OperationContext.IF_EXISTS_CHECK_PRESENT);
    } else {
      operationContextValue.add(OperationContext.IF_EXISTS_CHECK_ABSENT);
    }
    return preparedStatementTypes.get(getSlotIndexForMutationContextPreparedStatement(
      operationContextValue));
  }

  /**
   * Generates a Boundstatement that can be executed for the given incoming tuple. This boundstatement is then
   * executed as a command
   * @param ps The prepared statement that was shortlisted to execute the given tuple
   * @param tuple The tuple that represents the current execution context
   * @param setNulls This represents the value whether the columns in the prepared statement need to be ignored or
   *                 considered
   * @return The boundstatement appropriately built
   */
  @SuppressWarnings(value = "unchecked")
  private BoundStatement processPayloadForExecution(final PreparedStatement ps, final UpsertExecutionContext tuple,
      final boolean setNulls)
  {
    BoundStatement boundStatement = ps.bind();
    Object pojoPayload = tuple.getPayload();
    for (String cassandraColName : getters.keySet()) {
      DataType dataType = columnDefinitions.get(cassandraColName);
      switch (dataType.getName()) {
        case BOOLEAN:
          PojoUtils.GetterBoolean<Object> boolGetter = ((PojoUtils.GetterBoolean<Object>)getters
              .get(cassandraColName));
          if (boolGetter != null) {
            final boolean bool = boolGetter.get(pojoPayload);
            boundStatement.setBool(cassandraColName, bool);
          } else {
            boundStatement.unset(cassandraColName);
          }
          break;
        case INT:
          PojoUtils.GetterInt<Object> inGetter = ((PojoUtils.GetterInt<Object>)getters.get(cassandraColName));
          if (inGetter != null) {
            final int intValue = inGetter.get(pojoPayload);
            boundStatement.setInt(cassandraColName, intValue);
          } else {
            boundStatement.unset(cassandraColName);
          }
          break;
        case BIGINT:
        case COUNTER:
          PojoUtils.GetterLong<Object> longGetter = ((PojoUtils.GetterLong<Object>)getters.get(cassandraColName));
          if (longGetter != null) {
            final long longValue = longGetter.get(pojoPayload);
            boundStatement.setLong(cassandraColName, longValue);
          } else {
            boundStatement.unset(cassandraColName);
          }
          break;
        case FLOAT:
          PojoUtils.GetterFloat<Object> floatGetter = ((PojoUtils.GetterFloat<Object>)getters.get(cassandraColName));
          if (floatGetter != null) {
            final float floatValue = floatGetter.get(pojoPayload);
            boundStatement.setFloat(cassandraColName, floatValue);
          } else {
            boundStatement.unset(cassandraColName);
          }
          break;
        case DOUBLE:
          PojoUtils.GetterDouble<Object> doubleGetter = ((PojoUtils.GetterDouble<Object>)getters
              .get(cassandraColName));
          if (doubleGetter != null) {
            final double doubleValue = doubleGetter.get(pojoPayload);
            boundStatement.setDouble(cassandraColName, doubleValue);
          } else {
            boundStatement.unset(cassandraColName);
          }
          break;
        case DECIMAL:
          PojoUtils.Getter<Object, BigDecimal> bigDecimalGetter = ((PojoUtils.Getter<Object, BigDecimal>)getters
              .get(cassandraColName));
          if (bigDecimalGetter != null) {
            final BigDecimal decimal = bigDecimalGetter.get(pojoPayload);
            if (decimal == null) {
              if (!setNulls) {
                boundStatement.unset(cassandraColName);
              } else {
                boundStatement.setDecimal(cassandraColName, null);
              }
            } else {
              boundStatement.setDecimal(cassandraColName, decimal);
            }
          } else {
            boundStatement.unset(cassandraColName);
          }
          break;
        case UUID:
          PojoUtils.Getter<Object, UUID> uuidGetter = ((PojoUtils.Getter<Object, UUID>)getters.get(cassandraColName));
          if (uuidGetter != null) {
            final UUID uuid = uuidGetter.get(pojoPayload);
            if (uuid == null) {
              if (!setNulls) {
                boundStatement.unset(cassandraColName);
              } else {
                boundStatement.setUUID(cassandraColName, null);
              }
            } else {
              boundStatement.setUUID(cassandraColName, uuid);
            }
          } else {
            boundStatement.unset(cassandraColName);
          }
          break;
        case ASCII:
        case VARCHAR:
        case TEXT:
          PojoUtils.Getter<Object, String> stringGetter = ((PojoUtils.Getter<Object, String>)getters
              .get(cassandraColName));
          if (stringGetter != null) {
            final String ascii = stringGetter.get(pojoPayload);
            if (ascii == null) {
              if (!setNulls) {
                boundStatement.unset(cassandraColName);
              } else {
                boundStatement.setString(cassandraColName, null);
              }
            } else {
              boundStatement.setString(cassandraColName, ascii);
            }
          } else {
            boundStatement.unset(cassandraColName);
          }
          break;
        case SET:
          PojoUtils.Getter<Object, Set<?>> getterForSet = ((PojoUtils.Getter<Object, Set<?>>)getters
              .get(cassandraColName));
          if (getterForSet != null) {
            final Set<?> set = getterForSet.get(pojoPayload);
            if (set == null) {
              if (!setNulls) {
                boundStatement.unset(cassandraColName);
              } else {
                boundStatement.setSet(cassandraColName, null);
              }
            } else {
              boundStatement.setSet(cassandraColName, set);
            }
          } else {
            boundStatement.unset(cassandraColName);
          }
          break;
        case MAP:
          PojoUtils.Getter<Object, Map<?, ?>> mapGetter = ((PojoUtils.Getter<Object, Map<?, ?>>)getters
              .get(cassandraColName));
          if (mapGetter != null) {
            final Map<?, ?> map = mapGetter.get(pojoPayload);
            if (map == null) {
              if (!setNulls) {
                boundStatement.unset(cassandraColName);
              } else {
                boundStatement.setMap(cassandraColName, null);
              }
            } else {
              boundStatement.setMap(cassandraColName, map);
            }
          } else {
            boundStatement.unset(cassandraColName);
          }
          break;
        case LIST:
          PojoUtils.Getter<Object, List<?>> listGetter = ((PojoUtils.Getter<Object, List<?>>)getters
              .get(cassandraColName));
          if (listGetter != null) {
            final List<?> list = listGetter.get(pojoPayload);
            if (list == null) {
              if (!setNulls) {
                boundStatement.unset(cassandraColName);
              } else {
                boundStatement.setList(cassandraColName, null);
              }
            } else {
              boundStatement.setList(cassandraColName, list);
            }
          } else {
            boundStatement.unset(cassandraColName);
          }
          break;
        case TIMESTAMP:
          PojoUtils.Getter<Object, Date> dateGetter = ((PojoUtils.Getter<Object, Date>)getters.get(cassandraColName));
          if (dateGetter != null) {
            final Date date = dateGetter.get(pojoPayload);
            if (date == null) {
              if (!setNulls) {
                boundStatement.unset(cassandraColName);
              } else {
                boundStatement.setMap(cassandraColName, null);
              }
            } else {
              boundStatement.setDate(cassandraColName, LocalDate.fromMillisSinceEpoch(date.getTime()));
            }
          } else {
            boundStatement.unset(cassandraColName);
          }
          break;
        case UDT:
          PojoUtils.Getter<Object, Object> udtGetter = ((PojoUtils.Getter<Object, Object>)getters
              .get(cassandraColName));
          if (udtGetter != null) {
            final Object udtPayload = udtGetter.get(pojoPayload);
            if (udtPayload == null) {
              if (!setNulls) {
                boundStatement.unset(cassandraColName);
              } else {
                boundStatement.setUDTValue(cassandraColName, null);
              }
            } else {
              boundStatement.set(cassandraColName, udtPayload, codecsForCassandraColumnNames
                  .get(cassandraColName).getJavaType().getRawType());
            }
          } else {
            boundStatement.unset(cassandraColName);
          }
          break;
        default:
          throw new RuntimeException("Type not supported for " + dataType.getName());
      }
    }
    return boundStatement;
  }

  /**
   * Primarily resets the per window counter metrics.
   * @param windowId The windowid as provided by the apex framework
   */
  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    totalIgnoresInThisWindow = 0;
    totalWriteTimeoutsInThisWindow = 0;
    totalWriteRetriesInThisWindow =  0;
    uniqueHostsWrittenToInCurrentWindow.clear();
    successfullWrites = 0;
    ignoredRequestsDuetoIfExistsCheck = 0;
    writesWithConsistencyOne = 0;
    writesWithConsistencyTwo = 0;
    writesWithConsistencyThree = 0;
    writesWithConsistencyAll = 0;
    writesWithConsistencyLocalOne = 0;
    writesWithConsistencyQuorum = 0;
    writesWithConsistencyLocalQuorum = 0;
    writeWithConsistencyLocalSerial = 0;
    writesWithConsistencyEachQuorum = 0;
    writesWithConsistencySerial = 0;
    writesWithConsistencyAny = 0;
    currentWindowId = windowId;
    if ( currentWindowId != Stateless.WINDOW_ID) {
      if (currentWindowId > reconcilingWindowId) {
        isInSafeMode = false;
        isInReconcilingMode = false;
      }
      if (currentWindowId == reconcilingWindowId) {
        isInReconcilingMode = true;
        isInSafeMode = false;
      }
      if (currentWindowId < reconcilingWindowId) {
        isInReconcilingMode = false;
        isInSafeMode = true;
      }
    }
  }

  /**
   * Builds the metrics that can be sent to Application master.
   * Note that some of the metrics are computed in the cassandra driver itself and hence just
   * extracted from the driver state itself.
   */
  @Override
  public void endWindow()
  {
    super.endWindow();
    Timer timerForThisWindow = session.getCluster().getMetrics().getRequestsTimer();
    totalWriteRequestsAttempted += timerForThisWindow.getCount();
    numberOfHostsWrittenTo = uniqueHostsWrittenToInCurrentWindow.size();
    fifteenMinuteWriteRateLatency = timerForThisWindow.getFifteenMinuteRate();
    fiveMinuteWriteRateLatency = timerForThisWindow.getFiveMinuteRate();
    oneMinuteWriteRateLatency = timerForThisWindow.getOneMinuteRate();
    meanWriteRateLatency = timerForThisWindow.getMeanRate();
    Metrics.Errors errors = session.getCluster().getMetrics().getErrorMetrics();
    totalIgnoresInThisWindow = errors.getIgnores().getCount() - totalIgnoresSinceStart;
    totalIgnoresSinceStart = errors.getIgnores().getCount();
    totalWriteTimeoutsInThisWindow = errors.getWriteTimeouts().getCount() - totalWriteTimeoutsSinceStart;
    totalWriteTimeoutsSinceStart = errors.getWriteTimeouts().getCount();
    totalWriteRetriesInThisWindow =  errors.getRetriesOnWriteTimeout().getCount() - totalWriteRetriesSinceStart;
    totalWriteRetriesSinceStart = errors.getRetriesOnWriteTimeout().getCount();
    try {
      // we do not need any particular state and hence reusing the window id itself
      windowDataManager.save(currentWindowId,currentWindowId);
    } catch (IOException e) {
      LOG.error("Error while persisting the current window state " + currentWindowId + " because " + e.getMessage());
      throw new RuntimeException(e.getMessage());
    }
  }

  @Override
  public void committed(long windowId)
  {
    if (null != windowDataManager) {
      try {
        windowDataManager.committed(windowId);
      } catch (IOException e) {
        LOG.error("Error while committing the window id " + windowId + " because " + e.getMessage());
        throw new RuntimeException(e.getMessage());
      }
    }
  }

  @Override
  public void beforeCheckpoint(long windowId)
  {
    // nothing to be done here. Prevent concrete implementations to be forced to implement this
  }

  @Override
  public void checkpointed(long windowId)
  {
    // Nothing to be done here. Concrete operators can override if needed.
  }

  /**
   * Initializes cassandra cluster connections as specified by the Connection State manager.
   * Aspects that are initialized here include Identifying primary key column names, non-primary key columns,
   * collection type columns, counter columns
   * It also queries the Keyspace and Table metadata for extracting the above information.
   * It finally prepares all possible prepared statements that can be executed in the lifetime of the operator
   * for various permutations like APPEND/REMOVE COLLECTIONS , LIST APPEND/PREPEND , set nulls, set TTLs etc
   * @param context The apex framework context
   */
  @Override
  public void activate(Context.OperatorContext context)
  {
    ConnectionStateManager.ConnectionBuilder connectionBuilder = withConnectionBuilder();
    checkNotNull(connectionBuilder, " Connection Builder cannot be null.");
    connectionStateManager = connectionBuilder.initialize();
    cluster = connectionStateManager.getCluster();
    session = connectionStateManager.getSession();
    keyspaceName = connectionStateManager.getKeyspaceName();
    tableName = connectionStateManager.getTableName();
    checkNotNull(session, "Cassandra session cannot be null");
    tuplePayloadClass = getPayloadPojoClass();
    columnDefinitions = new HashMap<>();
    dataTypeGroupedColumns = new HashMap<>();
    counterColumns = new HashSet<>();
    collectionColumns = new HashSet<>();
    pkColumnNames = new HashSet<>();
    listColumns = new HashSet<>();
    mapColumns = new HashSet<>();
    setColumns = new HashSet<>();
    codecsForCassandraColumnNames = new HashMap<>();
    userDefinedTypeColumns = new HashSet<>();
    regularColumns = new HashSet<>();
    colNamesMap = new HashMap<>();
    getters = new HashMap<>();
    userDefinedTypesClass = new HashMap<>();
    uniqueHostsWrittenToInCurrentWindow = new HashSet<>();
    pojoFieldNamesToCassandraColumns = getOverridingPojoFieldsToCassandraColumnNames();
    if (pojoFieldNamesToCassandraColumns == null) {
      pojoFieldNamesToCassandraColumns = new HashMap<>();
    }
    registerCodecs();
    KeyspaceMetadata keyspaceMetadata = cluster.getMetadata()
        .getKeyspace(connectionStateManager.getKeyspaceName());
    TableMetadata tableMetadata = keyspaceMetadata
        .getTable(connectionStateManager.getTableName());
    registerPrimaryKeyColumnDefinitions(tableMetadata);
    registerNonPKColumnDefinitions(tableMetadata);
    preparedStatementTypes = new HashMap<>();
    generatePreparedStatements();
    registerGettersForPayload();
    if (null != windowDataManager) {
      if ( (context.getValue(Context.OperatorContext.ACTIVATION_WINDOW_ID) != Stateless.WINDOW_ID) &&
          context.getValue(Context.OperatorContext.ACTIVATION_WINDOW_ID) <
          windowDataManager.getLargestCompletedWindow()) {
        isInSafeMode = true;
        reconcilingWindowId = windowDataManager.getLargestCompletedWindow() + 1;
        isInReconcilingMode = false;
      }
    } else {
      isInSafeMode = false;
      isInReconcilingMode = false;
      reconcilingWindowId = Stateless.WINDOW_ID;
    }
  }

  @Override
  public void deactivate()
  {
    connectionStateManager.close();
  }

  /**
   * Builds th map that manages the getters for a given cassandra column
   * Aspects like case-insensitiveness , over-riding of column names to custom mappings
   */
  private void registerGettersForPayload()
  {
    Field[] classFields = tuplePayloadClass.getDeclaredFields();
    Set<String> allColNames = new HashSet<>();
    Map<String, DataType> dataTypeMap = new HashMap<>();
    Map<String,String> overridingColnamesMap = getPojoFieldNameToCassandraColumnNameOverride();
    if ( overridingColnamesMap == null) {
      overridingColnamesMap = new HashMap<>();
    }
    allColNames.addAll(pkColumnNames);
    allColNames.addAll(regularColumns);
    Set<String> normalizedColNames = new HashSet<>();
    Iterator<String> simpleColIterator = allColNames.iterator();
    while (simpleColIterator.hasNext()) {
      String aCol = simpleColIterator.next();
      normalizedColNames.add(aCol.toLowerCase());
      dataTypeMap.put(aCol.toLowerCase(), columnDefinitions.get(aCol));
      colNamesMap.put(aCol.toLowerCase(), aCol);
      codecsForCassandraColumnNames.put(aCol, complexTypeCodecs.get(aCol.toLowerCase()));
    }
    for (Field aField : classFields) {
      String aFieldName = aField.getName();
      if ( (normalizedColNames.contains(aFieldName.toLowerCase())) ||
          (overridingColnamesMap.containsKey(aFieldName)) ) {
        Object getter = null;
        String getterExpr = aFieldName;
        DataType returnDataTypeOfGetter = dataTypeMap.get(aFieldName.toLowerCase());
        if (returnDataTypeOfGetter == null) {
          returnDataTypeOfGetter = dataTypeMap.get(overridingColnamesMap.get(aFieldName));
        }
        switch (returnDataTypeOfGetter.getName()) {
          case INT:
            getter = PojoUtils.createGetterInt(tuplePayloadClass, getterExpr);
            break;
          case BIGINT:
          case COUNTER:
            getter = PojoUtils.createGetterLong(tuplePayloadClass, getterExpr);
            break;
          case ASCII:
          case TEXT:
          case VARCHAR:
            getter = PojoUtils.createGetter(tuplePayloadClass, getterExpr, String.class);
            break;
          case BOOLEAN:
            getter = PojoUtils.createGetterBoolean(tuplePayloadClass, getterExpr);
            break;
          case FLOAT:
            getter = PojoUtils.createGetterFloat(tuplePayloadClass, getterExpr);
            break;
          case DOUBLE:
            getter = PojoUtils.createGetterDouble(tuplePayloadClass, getterExpr);
            break;
          case DECIMAL:
            getter = PojoUtils.createGetter(tuplePayloadClass, getterExpr, BigDecimal.class);
            break;
          case SET:
            getter = PojoUtils.createGetter(tuplePayloadClass, getterExpr, Set.class);
            break;
          case MAP:
            getter = PojoUtils.createGetter(tuplePayloadClass, getterExpr, Map.class);
            break;
          case LIST:
            getter = PojoUtils.createGetter(tuplePayloadClass, getterExpr, List.class);
            break;
          case TIMESTAMP:
            getter = PojoUtils.createGetter(tuplePayloadClass, getterExpr, Date.class);
            break;
          case UUID:
            getter = PojoUtils.createGetter(tuplePayloadClass, getterExpr, UUID.class);
            break;
          case UDT:
            getter = PojoUtils.createGetter(tuplePayloadClass, getterExpr, userDefinedTypesClass.get(getterExpr));
            break;
          default:
            getter = PojoUtils.createGetter(tuplePayloadClass, getterExpr, Object.class);
            break;
        }
        String resolvedColumnName = colNamesMap.get(aFieldName.toLowerCase());
        if (overridingColnamesMap.containsKey(aFieldName)) {
          resolvedColumnName = overridingColnamesMap.get(aFieldName);
        }
        getters.put(resolvedColumnName, getter);
      }
    }
  }

  private void registerCodecs()
  {
    complexTypeCodecs = getCodecsForUserDefinedTypes();
    if (complexTypeCodecs != null) {
      CodecRegistry registry = cluster.getConfiguration().getCodecRegistry();
      if (cluster.getConfiguration().getProtocolOptions().getProtocolVersion().toInt() < 4) {
        LOG.error("Custom codecs are not supported for protocol version < 4");
        throw new RuntimeException("Custom codecs are not supported for protocol version < 4");
      }
      for (String typeCodecStr : complexTypeCodecs.keySet()) {
        TypeCodec codec = complexTypeCodecs.get(typeCodecStr);
        registry.register(codec);
        userDefinedTypesClass.put(typeCodecStr, codec.getJavaType().getRawType());
      }
    } else {
      complexTypeCodecs = new HashMap<>();
    }
  }

  private void registerNonPKColumnDefinitions(final TableMetadata tableMetadata)
  {
    List<ColumnMetadata> colInfoForTable = tableMetadata.getColumns();
    for (ColumnMetadata aColumnDefinition : colInfoForTable) {
      if (aColumnDefinition.getType().isCollection()) {
        collectionColumns.add(aColumnDefinition.getName());
      }
      if (!pkColumnNames.contains(aColumnDefinition.getName())) {
        columnDefinitions.put(aColumnDefinition.getName(), aColumnDefinition.getType());
        regularColumns.add(aColumnDefinition.getName());
      }
      parseForSpecialDataType(aColumnDefinition);
    }
  }

  private void parseForSpecialDataType(final ColumnMetadata aColumnDefinition)
  {
    switch (aColumnDefinition.getType().getName()) {
      case COUNTER:
        counterColumns.add(aColumnDefinition.getName());
        break;
      case MAP:
        mapColumns.add(aColumnDefinition.getName());
        break;
      case SET:
        setColumns.add(aColumnDefinition.getName());
        break;
      case LIST:
        listColumns.add(aColumnDefinition.getName());
        break;
      case UDT:
        userDefinedTypeColumns.add(aColumnDefinition.getName());
        break;
      default:
        break;
    }
  }

  private void registerPrimaryKeyColumnDefinitions(final TableMetadata tableMetadata)
  {
    List<ColumnMetadata> primaryKeyColumns = tableMetadata.getPrimaryKey();
    for (ColumnMetadata primaryColumn : primaryKeyColumns) {
      columnDefinitions.put(primaryColumn.getName(), primaryColumn.getType());
      pkColumnNames.add(primaryColumn.getName());
      parseForSpecialDataType(primaryColumn);
    }
  }

  private void generatePreparedStatements()
  {
    Map<Long, String> stringsWithoutPKAndExistsClauses = generatePreparedStatementsQueryStrings();
    String ifExistsClause = " IF EXISTS";
    Map<Long, String> finalSetOfQueryStrings = new HashMap<>();
    for (Long currentIndexPos : stringsWithoutPKAndExistsClauses.keySet()) {
      StringBuilder aQueryStub = new StringBuilder(stringsWithoutPKAndExistsClauses.get(currentIndexPos));
      buildWhereClauseForPrimaryKeys(aQueryStub);
      finalSetOfQueryStrings.put(currentIndexPos +
          getSlotIndexForMutationContextPreparedStatement(EnumSet.of(OperationContext.IF_EXISTS_CHECK_ABSENT)),
          aQueryStub.toString());
      if (counterColumns.size() == 0) {
        // IF exists cannot be used for counter column tables
        finalSetOfQueryStrings.put(currentIndexPos +
            getSlotIndexForMutationContextPreparedStatement(EnumSet.of(
            OperationContext.IF_EXISTS_CHECK_PRESENT)), aQueryStub.toString() + ifExistsClause);
      }

    }
    for (Long currentIndexPos : finalSetOfQueryStrings.keySet()) {
      String currentQueryStr = finalSetOfQueryStrings.get(currentIndexPos);
      LOG.info("Registering query support for " + currentQueryStr);
      PreparedStatement preparedStatementForThisQuery = session.prepare(currentQueryStr);
      preparedStatementTypes.put(currentIndexPos, preparedStatementForThisQuery);
    }
  }

  private Map<Long, String> generatePreparedStatementsQueryStrings()
  {
    Map<Long, String> queryStrings = new HashMap<>();
    //UPDATE keyspace_name.table_name USING option AND option SET assignment, assignment, ... WHERE row_specification
    StringBuilder updateQueryRoot = new StringBuilder(" UPDATE " + connectionStateManager.getKeyspaceName() +
        "." + connectionStateManager.getTableName() + " ");
    String ttlSetString = " USING ttl :" + TTL_PARAM_NAME + " SET ";
    // TTL set , Collections Append , List prepend
    StringBuilder queryExpTTLSetCollAppendListPrepend = new StringBuilder(updateQueryRoot.toString());
    queryExpTTLSetCollAppendListPrepend.append(ttlSetString);
    buildNonPKColumnsExpression(queryExpTTLSetCollAppendListPrepend,
        UpsertExecutionContext.ListPlacementStyle.PREPEND_TO_EXISTING_LIST,
        UpsertExecutionContext.CollectionMutationStyle.ADD_TO_EXISTING_COLLECTION);
    queryStrings.put(getSlotIndexForMutationContextPreparedStatement(EnumSet.of(
        OperationContext.TTL_SET, OperationContext.COLLECTIONS_APPEND, OperationContext.LIST_PREPEND
    )), queryExpTTLSetCollAppendListPrepend.toString());

    // TTL set , Collections Append , List append
    StringBuilder queryExpTTLSetCollAppendListAppend = new StringBuilder(updateQueryRoot.toString());
    queryExpTTLSetCollAppendListAppend.append(ttlSetString);
    buildNonPKColumnsExpression(queryExpTTLSetCollAppendListAppend,
        UpsertExecutionContext.ListPlacementStyle.APPEND_TO_EXISTING_LIST,
        UpsertExecutionContext.CollectionMutationStyle.ADD_TO_EXISTING_COLLECTION);
    queryStrings.put(getSlotIndexForMutationContextPreparedStatement(EnumSet.of(
        OperationContext.TTL_SET, OperationContext.COLLECTIONS_APPEND, OperationContext.LIST_APPEND
    )), queryExpTTLSetCollAppendListAppend.toString());

    // TTL set , Collections Remove
    StringBuilder queryExpTTLSetCollRemove = new StringBuilder(updateQueryRoot.toString());
    queryExpTTLSetCollRemove.append(ttlSetString);
    buildNonPKColumnsExpression(queryExpTTLSetCollRemove,
        UpsertExecutionContext.ListPlacementStyle.APPEND_TO_EXISTING_LIST, // Just in case user sets it
        UpsertExecutionContext.CollectionMutationStyle.REMOVE_FROM_EXISTING_COLLECTION);
    queryStrings.put(getSlotIndexForMutationContextPreparedStatement(EnumSet.of(
        OperationContext.TTL_SET, OperationContext.COLLECTIONS_REMOVE, OperationContext.LIST_APPEND
    )), queryExpTTLSetCollRemove.toString());

    // TTL Not set , Collections Append , List prepend
    StringBuilder queryExpTTLNotSetCollAppendListPrepend = new StringBuilder(updateQueryRoot.toString());
    queryExpTTLNotSetCollAppendListPrepend.append(" SET ");
    buildNonPKColumnsExpression(queryExpTTLNotSetCollAppendListPrepend,
        UpsertExecutionContext.ListPlacementStyle.PREPEND_TO_EXISTING_LIST,
        UpsertExecutionContext.CollectionMutationStyle.ADD_TO_EXISTING_COLLECTION);
    queryStrings.put(getSlotIndexForMutationContextPreparedStatement(EnumSet.of(
        OperationContext.TTL_NOT_SET, OperationContext.COLLECTIONS_APPEND, OperationContext.LIST_PREPEND
    )), queryExpTTLNotSetCollAppendListPrepend.toString());

    // TTL Not set , Collections Append , List append
    StringBuilder queryExpTTLNotSetCollAppendListAppend = new StringBuilder(updateQueryRoot.toString());
    queryExpTTLNotSetCollAppendListAppend.append(" SET ");
    buildNonPKColumnsExpression(queryExpTTLNotSetCollAppendListAppend,
        UpsertExecutionContext.ListPlacementStyle.APPEND_TO_EXISTING_LIST,
        UpsertExecutionContext.CollectionMutationStyle.ADD_TO_EXISTING_COLLECTION);
    queryStrings.put(getSlotIndexForMutationContextPreparedStatement(EnumSet.of(
        OperationContext.TTL_NOT_SET, OperationContext.COLLECTIONS_APPEND, OperationContext.LIST_APPEND
    )), queryExpTTLNotSetCollAppendListAppend.toString());

    // TTL Not set , Collections Remove
    StringBuilder queryExpTTLNotSetCollRemove = new StringBuilder(updateQueryRoot.toString());
    queryExpTTLNotSetCollRemove.append(" SET ");
    buildNonPKColumnsExpression(queryExpTTLNotSetCollRemove,
        UpsertExecutionContext.ListPlacementStyle.APPEND_TO_EXISTING_LIST, // Just in case user sets it
        UpsertExecutionContext.CollectionMutationStyle.REMOVE_FROM_EXISTING_COLLECTION);
    queryStrings.put(getSlotIndexForMutationContextPreparedStatement(EnumSet.of(
        OperationContext.TTL_NOT_SET, OperationContext.COLLECTIONS_REMOVE, OperationContext.LIST_APPEND
    )), queryExpTTLNotSetCollRemove.toString());
    return queryStrings;
  }

  private long getSlotIndexForMutationContextPreparedStatement(final EnumSet<OperationContext> context)
  {
    Iterator<OperationContext> itrForContexts = context.iterator();
    long indexValue = 0;
    while (itrForContexts.hasNext()) {
      OperationContext aContext = itrForContexts.next();
      indexValue += Math.pow(10, aContext.ordinal());
    }
    return indexValue;
  }

  protected Map<String, String> getOverridingPojoFieldsToCassandraColumnNames()
  {
    return new HashMap<>();
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

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    windowDataManager = getWindowDataManagerImpl();
    if (null != windowDataManager) {
      windowDataManager.setup(context);
    }
  }

  @Override
  public void teardown()
  {
    super.teardown();
    if (null != windowDataManager) {
      windowDataManager.teardown();
    }
  }

  public Map<String, DataType> getColumnDefinitions()
  {
    return columnDefinitions;
  }

  public void setColumnDefinitions(final Map<String, DataType> columnDefinitions)
  {
    this.columnDefinitions = columnDefinitions;
  }

  public Map<String, Class> getUserDefinedTypesClass()
  {
    return userDefinedTypesClass;
  }

  public void setUserDefinedTypesClass(final Map<String, Class> userDefinedTypesClass)
  {
    this.userDefinedTypesClass = userDefinedTypesClass;
  }

  public Set<String> getPkColumnNames()
  {
    return pkColumnNames;
  }

  public void setPkColumnNames(final Set<String> pkColumnNames)
  {
    this.pkColumnNames = pkColumnNames;
  }

  public Set<String> getCounterColumns()
  {
    return counterColumns;
  }

  public void setCounterColumns(final Set<String> counterColumns)
  {
    this.counterColumns = counterColumns;
  }

  public Set<String> getCollectionColumns()
  {
    return collectionColumns;
  }

  public void setCollectionColumns(final Set<String> collectionColumns)
  {
    this.collectionColumns = collectionColumns;
  }

  public Set<String> getListColumns()
  {
    return listColumns;
  }

  public void setListColumns(final Set<String> listColumns)
  {
    this.listColumns = listColumns;
  }

  public Set<String> getMapColumns()
  {
    return mapColumns;
  }

  public void setMapColumns(Set<String> mapColumns)
  {
    this.mapColumns = mapColumns;
  }

  public Set<String> getSetColumns()
  {
    return setColumns;
  }

  public void setSetColumns(Set<String> setColumns)
  {
    this.setColumns = setColumns;
  }

  public Set<String> getUserDefinedTypeColumns()
  {
    return userDefinedTypeColumns;
  }

  public void setUserDefinedTypeColumns(Set<String> userDefinedTypeColumns)
  {
    this.userDefinedTypeColumns = userDefinedTypeColumns;
  }

  public Set<String> getRegularColumns()
  {
    return regularColumns;
  }

  public void setRegularColumns(Set<String> regularColumns)
  {
    this.regularColumns = regularColumns;
  }

  public Map<Long, PreparedStatement> getPreparedStatementTypes()
  {
    return preparedStatementTypes;
  }

  public void setPreparedStatementTypes(Map<Long, PreparedStatement> preparedStatementTypes)
  {
    this.preparedStatementTypes = preparedStatementTypes;
  }

  public Map<String, Object> getGetters()
  {
    return getters;
  }

  public void setGetters(Map<String, Object> getters)
  {
    this.getters = getters;
  }

  /***
   * Implementing concrete Operator instances define the Connection Builder properties by implementing this method
   * Please refer to {@link com.datatorrent.contrib.cassandra.ConnectionStateManager.ConnectionBuilder} for
   * an example implementation of the ConnectionStateManager instantiation.
   * @return The connection state manager that is to be used for this Operator.
   */
  public abstract ConnectionStateManager.ConnectionBuilder withConnectionBuilder();

  /**
   * The implementing concrete operator needs to implement this map. The key is the name of the cassandra column
   * that this codec is used to map to. The TypeCode is used to represent the codec for that column in cassandra
   * Please refer to test example  UserUpsertOperator.java for implementation.
   * Concrete implementations can return a null if there are no user defined types
   * @return A map giving column name to the codec to be used
  */
  public abstract Map<String, TypeCodec> getCodecsForUserDefinedTypes();

  /**
   * Defines the Pojo class that is used to represent the row in the table that is set in the ConnectionStateManager
   * instance. The Class that is returned here should match the template type of the execution context
   * {@link UpsertExecutionContext}
   * @return The class that is used as the payload of the Execution context.
     */
  public abstract Class getPayloadPojoClass();

  /**
   * Concrete implementations can override this method to provide a custom map of a POJO file name to the cassandra
   * column name. This is useful when POJOs that are acting as payloads
   * 1. Cannot comply with code conventions of POJO as opposed to cassandra column names Ex: Cassandra column names
   *    might have underscores and POJO fields might not be in that format.
   *    It may be noted case sensitivity is ignored when trying to match Cassandra Column names
   * {@code
   *  @Override
      protected Map<String, String> getPojoFieldNameToCassandraColumnNameOverride()
      {
        Map<String,String> overridingColMap = new HashMap<>();
        overridingColMap.put("topScores","top_scores"); // topScores is POJO field name and top_scores is Cassandra col
        return overridingColMap;
      }
   *
   * }
   * @return A map of the POJO field name as key and value as the Cassandra Column name
     */
  protected Map<String,String> getPojoFieldNameToCassandraColumnNameOverride()
  {
    return new HashMap<>();
  }

  /***
   * Allows the concrete implementations to override the preferred sink for the WindowDataManager. This might
   * come in handy when apex moves to a loosely coupled system from HDFS stores perhaps in the future.
   * @return
   */
  protected WindowDataManager getWindowDataManagerImpl()
  {
    return new FSWindowDataManager();
  }

  /**
   *
   * Since Cassandra is not a strictly transactional system and if the Apex operator crashes when a window is in
   * transit, we might be replaying the same data to be written to cassandra. In the event of such situations, we
   * would like the control to the concrete operator instance to resolve if they want to let the write happen
   * or simply skip it. Return true if the write needs to go through or return false to prevent the write
   * from happening.
   * Note that this check only happens for one window of data when an operator is resuming from a previous start
   * In the case of a restart from a previously checkpointed window, the operator simply runs in a "safe mode"
   * until it reaches the reconciliation window. This is the only window in which this method is called. Hence it
   * might be okay if this method is "heavy". For example the implementor can choose to read from cassandra for the
   * incoming record key entry and decide to let the write through or ignore it completely. This is on a per tuple
   * basis just for the reconciliation window only.  Post reconciliation window, the data simply flows through
   * without this check.
   * @param T
   * @param windowId
   * @return
   */
  abstract boolean reconcileRecord(Object T, long windowId);

  enum OperationContext
  {
    UNDEFINED,
    COLLECTIONS_APPEND,
    COLLECTIONS_REMOVE,
    LIST_APPEND,
    LIST_PREPEND,
    TTL_SET,
    TTL_NOT_SET,
    IF_EXISTS_CHECK_PRESENT,
    IF_EXISTS_CHECK_ABSENT,
  }

}
