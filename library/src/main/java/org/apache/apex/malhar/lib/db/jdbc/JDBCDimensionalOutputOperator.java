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
package org.apache.apex.malhar.lib.db.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.appdata.gpo.GPOMutable;
import org.apache.apex.malhar.lib.appdata.schemas.DimensionalConfigurationSchema;
import org.apache.apex.malhar.lib.appdata.schemas.FieldsDescriptor;
import org.apache.apex.malhar.lib.appdata.schemas.Type;
import org.apache.apex.malhar.lib.db.AbstractPassThruTransactionableStoreOutputOperator;
import org.apache.apex.malhar.lib.dimensions.DimensionsDescriptor;
import org.apache.apex.malhar.lib.dimensions.DimensionsEvent.Aggregate;
import org.apache.apex.malhar.lib.dimensions.DimensionsEvent.EventKey;
import org.apache.apex.malhar.lib.dimensions.aggregator.AggregatorRegistry;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.api.Context;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

/**
 * This operator writes updates emitted by a {@link DimensionsStoreHDHT}
 * operator to a Mysql database. Updates are written to the database in the
 * following fashion: <br/>
 * <br/>
 * <ol>
 * <li>Aggregates are received from an upstream
 * {@link AbstractDimensionsComputationFlexibleSingleSchema} operator.</li>
 * <li>Each aggregate is written to a different table based on its dimension
 * combination, time bucket, and corresponding aggregation</li>
 * </ol>
 *
 * @since 3.4.0
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class JDBCDimensionalOutputOperator
    extends AbstractPassThruTransactionableStoreOutputOperator<Aggregate, JdbcTransactionalStore>
{
  protected static int DEFAULT_BATCH_SIZE = 1000;

  @Min(1)
  private int batchSize;
  private final List<Aggregate> tuples;

  private transient int batchStartIdx;

  @NotNull
  private Map<Integer, Map<String, String>> tableNames;
  @NotNull
  private String eventSchema;
  @NotNull
  private AggregatorRegistry aggregatorRegistry = AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY;
  private DimensionalConfigurationSchema schema;

  private transient Map<Integer, Map<Integer, PreparedStatement>> ddIDToAggIDToStatement = Maps.newHashMap();

  public JDBCDimensionalOutputOperator()
  {
    tuples = Lists.newArrayList();
    batchSize = DEFAULT_BATCH_SIZE;
    batchStartIdx = 0;
    store = new JdbcTransactionalStore();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);

    LOG.info("Done setting up super");
    aggregatorRegistry.setup();

    //Create prepared statements
    schema = new DimensionalConfigurationSchema(eventSchema, aggregatorRegistry);

    List<FieldsDescriptor> keyFDs = schema.getDimensionsDescriptorIDToKeyDescriptor();

    for (int ddID = 0; ddID < keyFDs.size(); ddID++) {

      LOG.info("ddID {}", ddID);
      FieldsDescriptor keyFD = keyFDs.get(ddID);
      Int2ObjectMap<FieldsDescriptor> aggIDToAggFD = schema
          .getDimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor().get(ddID);

      Map<Integer, PreparedStatement> aggIDToStatement = ddIDToAggIDToStatement.get(ddID);

      if (aggIDToStatement == null) {
        aggIDToStatement = Maps.newHashMap();
        ddIDToAggIDToStatement.put(ddID, aggIDToStatement);
      }

      for (Map.Entry<String, String> aggTable : tableNames.get(ddID).entrySet()) {
        int aggID = aggregatorRegistry.getIncrementalAggregatorNameToID().get(aggTable.getKey());

        LOG.info("aggID {}", aggID);
        FieldsDescriptor aggFD = aggIDToAggFD.get(aggID);

        List<String> keyNames = keyFD.getFieldList();
        keyNames.remove(DimensionsDescriptor.DIMENSION_TIME_BUCKET);

        LOG.info("List fields {}", keyNames);
        List<String> aggregateNames = aggFD.getFieldList();
        LOG.info("List fields {}", aggregateNames);
        String tableName = aggTable.getValue();

        String statementString = buildStatement(tableName, keyNames, aggregateNames);

        try {
          aggIDToStatement.put(aggID, store.getConnection().prepareStatement(statementString));
        } catch (SQLException ex) {
          throw new RuntimeException(ex);
        }
      }
    }
  }

  private String buildStatement(String tableName, List<String> keyNames, List<String> aggregateNames)
  {
    LOG.info("building statement");
    StringBuilder sb = new StringBuilder();
    sb.append("INSERT INTO ");
    sb.append(tableName);
    sb.append(" (");

    addList(sb, keyNames);
    sb.append(",");
    addList(sb, aggregateNames);

    sb.append(") VALUES (");

    for (int qCounter = 0;; qCounter++) {
      sb.append("?");

      if (qCounter == keyNames.size() + aggregateNames.size() - 1) {
        break;
      }

      sb.append(",");
    }

    sb.append(") ON DUPLICATE KEY UPDATE ");

    addOnDuplicate(sb, aggregateNames);

    return sb.toString();
  }

  private void addOnDuplicate(StringBuilder sb, List<String> names)
  {
    LOG.info("add Duplicate");
    for (int index = 0;; index++) {

      String name = names.get(index);
      sb.append(name);
      sb.append("=");
      sb.append("VALUES(");
      sb.append(name);
      sb.append(")");

      if (index == names.size() - 1) {
        break;
      }

      sb.append(",");
    }
  }

  private void addList(StringBuilder sb, List<String> names)
  {
    for (int index = 0;; index++) {
      sb.append(names.get(index));

      if (index == names.size() - 1) {
        break;
      }

      sb.append(",");
    }
  }

  /**
   * This sets the table names that corresponds to the dimensions combinations
   * specified in your {@link DimensionalConfigurationSchema}. The structure of
   * this property is as follows: <br/>
   * <br/>
   * <ol>
   * <li>The first key is the dimension combination id assigned to a dimension
   * combination in your {@link DimensionalConfigurationSchema}. <br/>
   * <br/>
   * The dimensions descriptor id is determined by the following factors:
   * <ul>
   * <li>The dimensions combination specified in the
   * {@link DimensionalConfigurationSchema}.</li>
   * <li>The the Time Buckets defined in your
   * {@link DimensionalConfigurationSchema}.</li>
   * </ul>
   * The dimensions descriptor id is computed in the following way:
   * <ol>
   * <li>The first dimensions descriptor id is 0</li>
   * <li>A dimension combination is selected</li>
   * <li>A time bucket is selected</li>
   * <li>The current dimension combination and time bucket pair is assigned a
   * dimensions descriptor id</li>
   * <li>The current dimensions descriptor id is incremented</li>
   * <li>Steps 3 - 5 are repeated until all the time buckets are done</li>
   * <li>Steps 2 - 6 are repeated until all the dimension combinations are done.
   * </li>
   * </ol>
   * <br/>
   * <</li>
   * <li>The second key is the name of an aggregation being performed for that
   * dimensions combination.</li>
   * <li>The value is the name of the output Mysql table</li>
   * </ol>
   *
   * @param tableNames
   *          The table names that corresponds to the dimensions combinations
   *          specified in your {@link DimensionalConfigurationSchema}.
   */
  public void setTableNames(Map<Integer, Map<String, String>> tableNames)
  {
    this.tableNames = Preconditions.checkNotNull(tableNames);
  }

  /**
   * Sets the JSON corresponding to the {@link DimensionalConfigurationSchema}
   * which was set on the upstream {@link AppDataSingleSchemaDimensionStoreHDHT}
   * and {@link AbstractDimensionsComputationFlexibleSingleSchema} operators.
   *
   * @param eventSchema
   *          The JSON corresponding to the
   *          {@link DimensionalConfigurationSchema} which was set on the
   *          upstream {@link AppDataSingleSchemaDimensionStoreHDHT} and
   *          {@link AbstractDimensionsComputationFlexibleSingleSchema}
   *          operators.
   */
  public void setEventSchema(String eventSchema)
  {
    this.eventSchema = eventSchema;
  }

  /**
   * Sets the {@link AggregatorRegistry} that is used to determine what
   * aggregators correspond to what ids.
   *
   * @param aggregatorRegistry
   *          The {@link AggregatorRegistry} that is used to determine what
   *          aggregators correspond to what ids.
   */
  public void setAggregatorRegistry(AggregatorRegistry aggregatorRegistry)
  {
    this.aggregatorRegistry = aggregatorRegistry;
  }

  @Override
  public void endWindow()
  {
    //Process any remaining tuples.
    if (tuples.size() - batchStartIdx > 0) {
      processBatch();
    }
    super.endWindow();
    tuples.clear();
    batchStartIdx = 0;
  }

  @Override
  public void processTuple(Aggregate tuple)
  {
    tuples.add(tuple);
    if ((tuples.size() - batchStartIdx) >= batchSize) {
      processBatch();
    }
  }

  /**
   * Processes all the tuples in the batch once the batch size for the operator
   * is reached.
   */
  private void processBatch()
  {
    LOG.info("start {} end {}", batchStartIdx, tuples.size());
    try {
      for (int i = batchStartIdx; i < tuples.size(); i++) {
        setStatementParameters(tuples.get(i));
      }

      for (Map.Entry<Integer, Map<Integer, PreparedStatement>> ddIDToAggIDToStatementEntry : ddIDToAggIDToStatement
          .entrySet()) {
        for (Map.Entry<Integer, PreparedStatement> entry : ddIDToAggIDToStatementEntry.getValue().entrySet()) {
          entry.getValue().executeBatch();
          entry.getValue().clearBatch();
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException("processing batch", e);
    } finally {
      batchStartIdx += tuples.size() - batchStartIdx;
    }
  }

  /**
   * Sets the parameters on the {@link java.sql.PreparedStatement} based on the
   * values in the given {@link Aggregate}.
   *
   * @param aggregate
   *          The {@link Aggregate} whose values will be set on the
   *          corresponding {@link java.sql.PreparedStatement}.
   */
  private void setStatementParameters(Aggregate aggregate)
  {
    EventKey eventKey = aggregate.getEventKey();

    int ddID = eventKey.getDimensionDescriptorID();
    int aggID = eventKey.getAggregatorID();

    LOG.info("Setting statement params {} {}", ddID, aggID);

    FieldsDescriptor keyFD = schema.getDimensionsDescriptorIDToKeyDescriptor().get(ddID);
    FieldsDescriptor aggFD = schema.getDimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor().get(ddID)
        .get(aggID);

    GPOMutable key = eventKey.getKey();
    key.setFieldDescriptor(keyFD);

    GPOMutable value = aggregate.getAggregates();
    value.setFieldDescriptor(aggFD);

    int qCounter = 1;

    PreparedStatement ps = ddIDToAggIDToStatement.get(ddID).get(aggID);

    try {
      qCounter = setParams(ps, key, qCounter, true);
      setParams(ps, value, qCounter, false);
      ps.addBatch();
    } catch (SQLException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * @param ps
   *          The {@link java.sql.PreparedStatement} which will do an insert
   *          into the Mysql database.
   * @param gpo
   *          The {@link GPOMutable} object whose values need to be set in the
   *          preparted statement.
   * @param qCounter
   *          The current index in the prepared statement
   * @param isKey
   *          TODO use this
   * @return The current index in the prepared statement.
   * @throws SQLException
   */
  private int setParams(PreparedStatement ps, GPOMutable gpo, int qCounter, boolean isKey) throws SQLException
  {
    FieldsDescriptor fd = gpo.getFieldDescriptor();

    Map<String, Type> fieldToType = fd.getFieldToType();
    List<String> fields = fd.getFieldList();

    for (int fieldCounter = 0; fieldCounter < fields.size(); fieldCounter++, qCounter++) {
      String fieldName = fields.get(fieldCounter);

      if (fieldName.equals(DimensionsDescriptor.DIMENSION_TIME_BUCKET)) {
        qCounter--;
        continue;
      }

      Type type = fieldToType.get(fieldName);

      LOG.info("Field Name {} {}", fieldName, qCounter);

      switch (type) {
        case BOOLEAN: {
          ps.setByte(qCounter, (byte)(gpo.getFieldBool(fieldName) ? 1 : 0));
          break;
        }
        case BYTE: {
          ps.setByte(qCounter, gpo.getFieldByte(fieldName));
          break;
        }
        case CHAR: {
          ps.setString(qCounter, Character.toString(gpo.getFieldChar(fieldName)));
          break;
        }
        case STRING: {
          ps.setString(qCounter, gpo.getFieldString(fieldName));
          break;
        }
        case SHORT: {
          ps.setInt(qCounter, gpo.getFieldShort(fieldName));
          break;
        }
        case INTEGER: {
          ps.setInt(qCounter, gpo.getFieldInt(fieldName));
          break;
        }
        case LONG: {
          ps.setLong(qCounter, gpo.getFieldLong(fieldName));
          break;
        }
        case FLOAT: {
          ps.setFloat(qCounter, gpo.getFieldFloat(fieldName));
          break;
        }
        case DOUBLE: {
          ps.setDouble(qCounter, gpo.getFieldDouble(fieldName));
          break;
        }
        default: {
          throw new UnsupportedOperationException("The type: " + type + " is not supported.");
        }
      }
    }

    return qCounter;
  }

  /**
   * Sets the size of a batch operation.<br/>
   * <b>Default:</b> {@value #DEFAULT_BATCH_SIZE}
   *
   * @param batchSize
   *          size of a batch
   */
  public void setBatchSize(int batchSize)
  {
    this.batchSize = batchSize;
  }

  private static final Logger LOG = LoggerFactory.getLogger(AbstractJdbcTransactionableOutputOperator.class);
}

