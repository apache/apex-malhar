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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.apex.malhar.lib.util.FieldInfo;
import org.apache.apex.malhar.lib.util.PojoUtils;
import org.apache.apex.malhar.lib.util.PojoUtils.Setter;
import org.apache.apex.malhar.lib.util.PojoUtils.SetterBoolean;
import org.apache.apex.malhar.lib.util.PojoUtils.SetterDouble;
import org.apache.apex.malhar.lib.util.PojoUtils.SetterFloat;
import org.apache.apex.malhar.lib.util.PojoUtils.SetterInt;
import org.apache.apex.malhar.lib.util.PojoUtils.SetterLong;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * <p>
 * CassandraPOJOInputOperator</p>
 * A generic implementation of AbstractCassandraInputOperator that fetches rows of data from Cassandra and emits them as POJOs.
 * Each row is converted to a POJO by mapping the columns in the row to fields of the POJO based on a user specified mapping.
 * User should also provide a query to fetch the rows from database. This query is run continuously to fetch new data and
 * hence should be parameterized. The parameters that can be used are %t for table name, %p for primary key, %s for start value
 * and %l for limit. The start value is continuously updated with the value of a primary key column of the last row from
 * the result of the previous run of the query. The primary key column is also identified by the user using a property.
 *
 * @displayName Cassandra Input Operator
 * @category Input
 * @tags database, nosql, pojo, cassandra
 * @since 3.0.0
 */
@Evolving
public class CassandraPOJOInputOperator extends AbstractCassandraInputOperator<Object> implements Operator.ActivationListener<OperatorContext>
{
  private String tokenQuery;
  @NotNull
  private List<FieldInfo> fieldInfos;
  private Number startRow;
  private Long startRowToken = Long.MIN_VALUE;
  @NotNull
  private String tablename;
  @NotNull
  private String query;
  @NotNull
  private String primaryKeyColumn;
  @Min(1)
  private int limit = 10;

  protected final transient List<Object> setters;
  protected final transient List<DataType> columnDataTypes;
  protected transient Class<?> pojoClass;

  @OutputPortFieldAnnotation(schemaRequired = true)
  public final transient DefaultOutputPort<Object> outputPort = new DefaultOutputPort<Object>()
  {
    @Override
    public void setup(Context.PortContext context)
    {
      pojoClass = context.getValue(Context.PortContext.TUPLE_CLASS);
    }
  };

  /**
   * Gets number of records to be fetched at one time from cassandra table.
   * @return limit
   */
  public int getLimit()
  {
    return limit;
  }

  /**
   * Sets number of records to be fetched at one time from cassandra table.
   * @param limit
   */
  public void setLimit(int limit)
  {
    this.limit = limit;
  }

  /*
   * Primary Key Column of table.
   * Gets the primary key column of Cassandra table.
   */
  public String getPrimaryKeyColumn()
  {
    return primaryKeyColumn;
  }

  public void setPrimaryKeyColumn(String primaryKeyColumn)
  {
    this.primaryKeyColumn = primaryKeyColumn;
  }


  /*
   * User has the option to specify the starting row of the range of data they desire.
   */
  public Number getStartRow()
  {
    return startRow;
  }

  public void setStartRow(Number startRow)
  {
    this.startRow = startRow;
  }

  /*
   * Parameterized query with parameters such as %t for table name , %p for primary key, %s for start value and %l for limit.
   * Example of retrieveQuery:
   * select * from %t where token(%p) > %s LIMIT %l;
   */
  public String getQuery()
  {
    return query;
  }

  public void setQuery(String query)
  {
    this.query = query;
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
   * @useSchema $[].pojoFieldExpression outputPort.fields[].name
   */
  public void setFieldInfos(List<FieldInfo> fieldInfos)
  {
    this.fieldInfos = fieldInfos;
  }

  /*
   * Tablename in cassandra.
   */
  public String getTablename()
  {
    return tablename;
  }

  public void setTablename(String tablename)
  {
    this.tablename = tablename;
  }

  public CassandraPOJOInputOperator()
  {
    super();
    columnDataTypes = new ArrayList<DataType>();
    setters = new ArrayList<Object>();
    this.store = new CassandraStore();
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    tokenQuery = "select token(" + primaryKeyColumn + ") from " + store.keyspace + "." + tablename + " where " + primaryKeyColumn + " =  ?";
  }

  @Override
  public void activate(OperatorContext context)
  {
    Long keyToken;
    if (startRow != null) {
      if ((keyToken = fetchKeyTokenFromDB(startRow)) != null) {
        startRowToken = keyToken;
      }
    }

    com.datastax.driver.core.ResultSet rs = store.getSession().execute("select * from " + store.keyspace + "." + tablename + " LIMIT " + 1);
    ColumnDefinitions rsMetaData = rs.getColumnDefinitions();

    if (query.contains("%t")) {
      query = query.replace("%t", tablename);
    }
    if (query.contains("%p")) {
      query = query.replace("%p", primaryKeyColumn);
    }
    if (query.contains("%l")) {
      query = query.replace("%l", limit + "");
    }

    LOG.debug("query is {}", query);

    for (FieldInfo fieldInfo : fieldInfos) {
      // Get the designated column's data type.
      DataType type = rsMetaData.getType(fieldInfo.getColumnName());
      columnDataTypes.add(type);
      Object setter;
      final String setterExpr = fieldInfo.getPojoFieldExpression();
      switch (type.getName()) {
        case ASCII:
        case TEXT:
        case VARCHAR:
          setter = PojoUtils.createSetter(pojoClass, setterExpr, String.class);
          break;
        case BOOLEAN:
          setter = PojoUtils.createSetterBoolean(pojoClass, setterExpr);
          break;
        case INT:
          setter = PojoUtils.createSetterInt(pojoClass, setterExpr);
          break;
        case BIGINT:
        case COUNTER:
          setter = PojoUtils.createSetterLong(pojoClass, setterExpr);
          break;
        case FLOAT:
          setter = PojoUtils.createSetterFloat(pojoClass, setterExpr);
          break;
        case DOUBLE:
          setter = PojoUtils.createSetterDouble(pojoClass, setterExpr);
          break;
        case DECIMAL:
          setter = PojoUtils.createSetter(pojoClass, setterExpr, BigDecimal.class);
          break;
        case SET:
          setter = PojoUtils.createSetter(pojoClass, setterExpr, Set.class);
          break;
        case MAP:
          setter = PojoUtils.createSetter(pojoClass, setterExpr, Map.class);
          break;
        case LIST:
          setter = PojoUtils.createSetter(pojoClass, setterExpr, List.class);
          break;
        case TIMESTAMP:
          setter = PojoUtils.createSetter(pojoClass, setterExpr, Date.class);
          break;
        case UUID:
          setter = PojoUtils.createSetter(pojoClass, setterExpr, UUID.class);
          break;
        default:
          setter = PojoUtils.createSetter(pojoClass, setterExpr, Object.class);
          break;
      }
      setters.add(setter);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object getTuple(Row row)
  {
    Object obj;

    try {
      // This code will be replaced after integration of creating POJOs on the fly utility.
      obj = pojoClass.newInstance();
    } catch (InstantiationException | IllegalAccessException ex) {
      throw new RuntimeException(ex);
    }

    for (int i = 0; i < columnDataTypes.size(); i++) {
      DataType type = columnDataTypes.get(i);
      String columnName = fieldInfos.get(i).getColumnName();
      switch (type.getName()) {
        case UUID:
          final UUID id = row.getUUID(columnName);
          ((Setter<Object, UUID>)setters.get(i)).set(obj, id);
          break;
        case ASCII:
        case VARCHAR:
        case TEXT:
          final String ascii = row.getString(columnName);
          ((Setter<Object, String>)setters.get(i)).set(obj, ascii);
          break;
        case BOOLEAN:
          final boolean bool = row.getBool(columnName);
          ((SetterBoolean)setters.get(i)).set(obj, bool);
          break;
        case INT:
          final int intValue = row.getInt(columnName);
          ((SetterInt)setters.get(i)).set(obj, intValue);
          break;

        case BIGINT:
        case COUNTER:
          final long longValue = row.getLong(columnName);
          ((SetterLong)setters.get(i)).set(obj, longValue);
          break;
        case FLOAT:
          final float floatValue = row.getFloat(columnName);
          ((SetterFloat)setters.get(i)).set(obj, floatValue);
          break;
        case DOUBLE:
          final double doubleValue = row.getDouble(columnName);
          ((SetterDouble)setters.get(i)).set(obj, doubleValue);
          break;
        case DECIMAL:
          final BigDecimal decimal = row.getDecimal(columnName);
          ((Setter<Object, BigDecimal>)setters.get(i)).set(obj, decimal);
          break;
        case SET:
          Set<?> set = row.getSet(columnName, Object.class);
          ((Setter<Object, Set<?>>)setters.get(i)).set(obj, set);
          break;
        case MAP:
          final Map<?, ?> map = row.getMap(columnName, Object.class, Object.class);
          ((Setter<Object, Map<?, ?>>)setters.get(i)).set(obj, map);
          break;
        case LIST:
          final List<?> list = row.getList(columnName, Object.class);
          ((Setter<Object, List<?>>)setters.get(i)).set(obj, list);
          break;
        case TIMESTAMP:
          final Date date = new Date(row.getDate(columnName).getMillisSinceEpoch());
          ((Setter<Object, Date>)setters.get(i)).set(obj, date);
          break;
        default:
          throw new RuntimeException("unsupported data type " + type.getName());
      }
    }
    return obj;
  }

  /*
   * This method replaces the parameters in Query with actual values given by user.
   * Example of retrieveQuery:
   * select * from %t where token(%p) > %v limit %l;
   */
  @Override
  public String queryToRetrieveData()
  {
    if (query.contains("%v")) {
      return query.replace("%v", startRowToken + "");
    }
    return query;
  }

  private Long fetchKeyTokenFromDB(Object keyValue)
  {
    PreparedStatement statement = store.getSession().prepare(tokenQuery);
    BoundStatement boundStatement = new BoundStatement(statement);
    boundStatement.bind(keyValue);
    ResultSet rs = store.getSession().execute(boundStatement);
    Long keyTokenValue = rs.one().getLong(0);
    return keyTokenValue;
  }

  @Override
  protected void emit(Object tuple)
  {
    outputPort.emit(tuple);
  }

  @Override
  public void deactivate()
  {
  }

  private static final Logger LOG = LoggerFactory.getLogger(CassandraPOJOInputOperator.class);
}
