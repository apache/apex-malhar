/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.GetterBoolean;
import com.datatorrent.lib.util.PojoUtils.GetterDouble;
import com.datatorrent.lib.util.PojoUtils.GetterFloat;
import com.datatorrent.lib.util.PojoUtils.GetterInt;
import com.datatorrent.lib.util.PojoUtils.GetterLong;
import com.datatorrent.lib.util.PojoUtils.Getter;
import java.math.BigDecimal;
import java.util.*;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * CassandraOutputOperator class.</p>
 * A Generic implementation of AbstractCassandraTransactionableOutputOperatorPS which takes in any POJO.
 *
 * @displayName Cassandra Output Operator
 * @category Output
 * @tags output operator
 * @since 2.1.0
 */
public class CassandraPOJOOutputOperator extends AbstractCassandraTransactionableOutputOperatorPS<Object>
{
  private static final long serialVersionUID = 201506181024L;
  @NotNull
  private ArrayList<String> columns;
  private final transient ArrayList<DataType> columnDataTypes;
  @NotNull
  private ArrayList<String> expressions;
  private final transient ArrayList<Object> getters;

  /*
   * An ArrayList of Java expressions that will yield the field value from the POJO.
   * Each expression corresponds to one column in the Cassandra table.
   */
  public ArrayList<String> getExpressions()
  {
    return expressions;
  }

  public void setExpressions(ArrayList<String> expressions)
  {
    this.expressions = expressions;
  }

  /*
   * An ArrayList of Columns in the Cassandra Table.
   */
  public ArrayList<String> getColumns()
  {
    return columns;
  }

  public void setColumns(ArrayList<String> columns)
  {
    this.columns = columns;
  }

  @NotNull
  private String tablename;


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

  public CassandraPOJOOutputOperator()
  {
    super();
    columnDataTypes = new ArrayList<DataType>();
    getters = new ArrayList<Object>();
  }

  public void processFirstTuple(Object tuple)
  {
    com.datastax.driver.core.ResultSet rs = store.getSession().execute("select * from " + store.keyspace + "." + tablename);

    final ColumnDefinitions rsMetaData = rs.getColumnDefinitions();

    final int numberOfColumns = rsMetaData.size();
    final Class<?> fqcn = tuple.getClass();

    for (int i = 0; i < numberOfColumns; i++) {
      // get the designated column's data type.
      final DataType type = rsMetaData.getType(i);
      columnDataTypes.add(type);
      final Object getter;
      final String getterExpr = expressions.get(i);
      switch (type.getName()) {
        case ASCII:
        case TEXT:
        case VARCHAR:
          getter = PojoUtils.createGetter(fqcn, getterExpr, String.class);
          break;
        case BOOLEAN:
          getter = PojoUtils.createGetterBoolean(fqcn, getterExpr);
          break;
        case INT:
          getter = PojoUtils.createGetterInt(fqcn, getterExpr);
          break;
        case BIGINT:
        case COUNTER:
          getter = PojoUtils.createGetterLong(fqcn, getterExpr);
          break;
        case FLOAT:
          getter = PojoUtils.createGetterFloat(fqcn, getterExpr);
          break;
        case DOUBLE:
          getter = PojoUtils.createGetterDouble(fqcn, getterExpr);
          break;
        case DECIMAL:
          getter = PojoUtils.createGetter(fqcn, getterExpr, BigDecimal.class);
          break;
        case SET:
          getter = PojoUtils.createGetter(fqcn, getterExpr, Set.class);
          break;
        case MAP:
          getter = PojoUtils.createGetter(fqcn, getterExpr, Map.class);
          break;
        case LIST:
          getter = PojoUtils.createGetter(fqcn, getterExpr, List.class);
          break;
        case TIMESTAMP:
          getter = PojoUtils.createGetter(fqcn, getterExpr, Date.class);
          break;
        case UUID:
          getter = PojoUtils.createGetter(fqcn, getterExpr, UUID.class);
          break;
        default:
          getter = PojoUtils.createGetter(fqcn, getterExpr, Object.class);
          break;
      }
      getters.add(getter);
    }
  }

  @Override
  protected PreparedStatement getUpdateCommand()
  {
    StringBuilder queryfields = new StringBuilder("");
    StringBuilder values = new StringBuilder("");
    for (String column: columns) {
      if (queryfields.length() == 0) {
        queryfields.append(column);
        values.append("?");
      }
      else {
        queryfields.append(",").append(column);
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
    if (getters.isEmpty()) {
      processFirstTuple(tuple);
    }
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
          boundStmnt.setDate(i, date);
          break;
        default:
          throw new RuntimeException("unsupported data type " + type.getName());
      }
    }
    return boundStmnt;
  }

  private static final Logger LOG = LoggerFactory.getLogger(CassandraPOJOOutputOperator.class);
}
