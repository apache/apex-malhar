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

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.util.FieldInfo;
import org.apache.apex.malhar.lib.util.PojoUtils;
import org.apache.apex.malhar.lib.util.PojoUtils.Getter;
import org.apache.apex.malhar.lib.util.PojoUtils.GetterBoolean;
import org.apache.apex.malhar.lib.util.PojoUtils.GetterDouble;
import org.apache.apex.malhar.lib.util.PojoUtils.GetterFloat;
import org.apache.apex.malhar.lib.util.PojoUtils.GetterInt;
import org.apache.apex.malhar.lib.util.PojoUtils.GetterLong;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.google.common.collect.Lists;
import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * <p>
 * CassandraOutputOperator class.</p>
 * A Generic implementation of AbstractCassandraTransactionableOutputOperatorPS which takes in any POJO.
 *
 * @displayName Cassandra Output Operator
 * @category Output
 * @tags database, nosql, pojo, cassandra
 * @since 2.1.0
 */
@Evolving
public class CassandraPOJOOutputOperator extends AbstractCassandraTransactionableOutputOperator<Object>
{
  private List<FieldInfo> fieldInfos;
  private String tablename;
  private String query;

  protected final transient ArrayList<DataType> columnDataTypes;
  protected final transient ArrayList<Object> getters;
  protected transient Class<?> pojoClass;

  @AutoMetric
  private long successfulRecords;
  @AutoMetric
  private long errorRecords;

  /**
   * The input port on which tuples are received for writing.
   */
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
  {
    @Override
    public void setup(Context.PortContext context)
    {
      pojoClass = context.getValue(Context.PortContext.TUPLE_CLASS);
    }

    @Override
    public void process(Object tuple)
    {
      CassandraPOJOOutputOperator.super.input.process(tuple);
    }

  };

  @OutputPortFieldAnnotation(error = true)
  public final transient DefaultOutputPort<Object> error = new DefaultOutputPort<>();

  public CassandraPOJOOutputOperator()
  {
    super();
    columnDataTypes = new ArrayList<DataType>();
    getters = new ArrayList<Object>();
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    successfulRecords = 0;
    errorRecords = 0;
  }

  @Override
  public void activate(Context.OperatorContext context)
  {
    com.datastax.driver.core.ResultSet rs
        = store.getSession().execute("select * from " + store.keyspace + "." + tablename);
    final ColumnDefinitions rsMetaData = rs.getColumnDefinitions();

    if (fieldInfos == null) {
      populateFieldInfosFromPojo(rsMetaData);
    }

    for (FieldInfo fieldInfo : getFieldInfos()) {
      // get the designated column's data type.
      final DataType type = rsMetaData.getType(fieldInfo.getColumnName());
      columnDataTypes.add(type);
      final Object getter;
      final String getterExpr = fieldInfo.getPojoFieldExpression();
      switch (type.getName()) {
        case ASCII:
        case TEXT:
        case VARCHAR:
          getter = PojoUtils.createGetter(pojoClass, getterExpr, String.class);
          break;
        case BOOLEAN:
          getter = PojoUtils.createGetterBoolean(pojoClass, getterExpr);
          break;
        case INT:
          getter = PojoUtils.createGetterInt(pojoClass, getterExpr);
          break;
        case BIGINT:
        case COUNTER:
          getter = PojoUtils.createGetterLong(pojoClass, getterExpr);
          break;
        case FLOAT:
          getter = PojoUtils.createGetterFloat(pojoClass, getterExpr);
          break;
        case DOUBLE:
          getter = PojoUtils.createGetterDouble(pojoClass, getterExpr);
          break;
        case DECIMAL:
          getter = PojoUtils.createGetter(pojoClass, getterExpr, BigDecimal.class);
          break;
        case SET:
          getter = PojoUtils.createGetter(pojoClass, getterExpr, Set.class);
          break;
        case MAP:
          getter = PojoUtils.createGetter(pojoClass, getterExpr, Map.class);
          break;
        case LIST:
          getter = PojoUtils.createGetter(pojoClass, getterExpr, List.class);
          break;
        case TIMESTAMP:
          getter = PojoUtils.createGetter(pojoClass, getterExpr, Date.class);
          break;
        case UUID:
          getter = PojoUtils.createGetter(pojoClass, getterExpr, UUID.class);
          break;
        default:
          getter = PojoUtils.createGetter(pojoClass, getterExpr, Object.class);
          break;
      }
      getters.add(getter);
    }
    super.activate(context);
  }

  private void populateFieldInfosFromPojo(ColumnDefinitions rsMetaData)
  {
    fieldInfos = Lists.newArrayList();
    Field[] fields = pojoClass.getDeclaredFields();
    for (int i = 0; i < rsMetaData.size(); i++) {
      String columnName = rsMetaData.getName(i);
      String pojoField = getMatchingField(fields, columnName);
      if (pojoField != null && pojoField.length() != 0) {
        fieldInfos.add(new FieldInfo(columnName, pojoField, null));
      } else {
        LOG.warn("Couldn't find corrosponding pojo field for column: " + columnName);
      }
    }
  }

  private String getMatchingField(Field[] fields, String columnName)
  {
    for (Field f : fields) {
      if (f.getName().equalsIgnoreCase(columnName)) {
        return f.getName();
      }
    }
    return null;
  }


  /**
   * {@inheritDoc} <br/>
   * If statement/query is not specified by user, insert query is constructed from fileInfo object and table name.
   */
  @Override
  protected PreparedStatement getUpdateCommand()
  {
    PreparedStatement statement;
    if (query == null) {
      statement = prepareStatementFromFieldsAndTableName();
    } else {
      statement = store.getSession().prepare(query);
    }
    LOG.debug("Statement is: " + statement.getQueryString());
    return statement;
  }

  private PreparedStatement prepareStatementFromFieldsAndTableName()
  {
    if (tablename == null || tablename.length() == 0) {
      throw new RuntimeException("Please sepcify query or table name.");
    }
    StringBuilder queryfields = new StringBuilder();
    StringBuilder values = new StringBuilder();
    for (FieldInfo fieldInfo: fieldInfos) {
      if (queryfields.length() == 0) {
        queryfields.append(fieldInfo.getColumnName());
        values.append("?");
      } else {
        queryfields.append(",").append(fieldInfo.getColumnName());
        values.append(",").append("?");
      }
    }
    String statement
        = "INSERT INTO " + store.keyspace + "."
        + tablename
        + " (" + queryfields.toString() + ") "
        + "VALUES (" + values.toString() + ");";
    LOG.debug("statement is {}", statement);
    return store.getSession().prepare(statement);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Statement setStatementParameters(PreparedStatement updateCommand, Object tuple) throws DriverException
  {
    final BoundStatement boundStmnt = new BoundStatement(updateCommand);
    final int size = columnDataTypes.size();
    for (int i = 0; i < size; i++) {
      final DataType type = columnDataTypes.get(i);
      switch (type.getName()) {
        case UUID:
          final UUID id = ((Getter<Object, UUID>)getters.get(i)).get(tuple);
          boundStmnt.setUUID(i, id);
          break;
        case ASCII:
        case VARCHAR:
        case TEXT:
          final String ascii = ((Getter<Object, String>)getters.get(i)).get(tuple);
          boundStmnt.setString(i, ascii);
          break;
        case BOOLEAN:
          final boolean bool = ((GetterBoolean<Object>)getters.get(i)).get(tuple);
          boundStmnt.setBool(i, bool);
          break;
        case INT:
          final int intValue = ((GetterInt<Object>)getters.get(i)).get(tuple);
          boundStmnt.setInt(i, intValue);
          break;
        case BIGINT:
        case COUNTER:
          final long longValue = ((GetterLong<Object>)getters.get(i)).get(tuple);
          boundStmnt.setLong(i, longValue);
          break;
        case FLOAT:
          final float floatValue = ((GetterFloat<Object>)getters.get(i)).get(tuple);
          boundStmnt.setFloat(i, floatValue);
          break;
        case DOUBLE:
          final double doubleValue = ((GetterDouble<Object>)getters.get(i)).get(tuple);
          boundStmnt.setDouble(i, doubleValue);
          break;
        case DECIMAL:
          final BigDecimal decimal = ((Getter<Object, BigDecimal>)getters.get(i)).get(tuple);
          boundStmnt.setDecimal(i, decimal);
          break;
        case SET:
          Set<?> set = ((Getter<Object, Set<?>>)getters.get(i)).get(tuple);
          boundStmnt.setSet(i, set);
          break;
        case MAP:
          final Map<?,?> map = ((Getter<Object, Map<?,?>>)getters.get(i)).get(tuple);
          boundStmnt.setMap(i, map);
          break;
        case LIST:
          final List<?> list = ((Getter<Object, List<?>>)getters.get(i)).get(tuple);
          boundStmnt.setList(i, list);
          break;
        case TIMESTAMP:
          final Date date = ((Getter<Object, Date>)getters.get(i)).get(tuple);
          boundStmnt.setDate(i, LocalDate.fromMillisSinceEpoch(date.getTime()));
          break;
        default:
          throw new RuntimeException("unsupported data type " + type.getName());
      }
    }
    return boundStmnt;
  }

  @Override
  public void processTuple(Object tuple)
  {
    try {
      super.processTuple(tuple);
      successfulRecords++;
    } catch (RuntimeException e) {
      LOG.error(e.getMessage());
      error.emit(tuple);
      errorRecords++;
    }
  }

  /**
   * A list of {@link FieldInfo}s where each item maps a column name to a pojo field name.
   */
  public List<FieldInfo> getFieldInfos()
  {
    return fieldInfos;
  }

  /**
   * Sets the {@link FieldInfo}s. A {@link FieldInfo} maps a store column to a pojo field name.<br/>
   * The value from fieldInfo.column is assigned to fieldInfo.pojoFieldExpression.
   *
   * @description $[].columnName name of the database column name
   * @description $[].pojoFieldExpression pojo field name or expression
   * @useSchema $[].pojoFieldExpression input.fields[].name
   */
  public void setFieldInfos(List<FieldInfo> fieldInfos)
  {
    this.fieldInfos = fieldInfos;
  }

  /**
   * Gets cassandra table name
   * @return tableName
   */
  public String getTablename()
  {
    return tablename;
  }

  /**
   * Sets cassandra table name (optional if query is specified)
   * @param tablename
   */
  public void setTablename(String tablename)
  {
    this.tablename = tablename;
  }

  /**
   * Gets cql Query
   * @return query
   */
  public String getQuery()
  {
    return query;
  }

  /**
   * Sets cql Query
   * @param query
   */
  public void setQuery(String query)
  {
    this.query = query;
  }

  private static final Logger LOG = LoggerFactory.getLogger(CassandraPOJOOutputOperator.class);
}
