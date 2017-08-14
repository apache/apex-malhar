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
package org.apache.apex.malhar.contrib.memsql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.apex.malhar.lib.util.PojoUtils;
import org.apache.apex.malhar.lib.util.PojoUtils.Getter;
import org.apache.apex.malhar.lib.util.PojoUtils.GetterBoolean;
import org.apache.apex.malhar.lib.util.PojoUtils.GetterChar;
import org.apache.apex.malhar.lib.util.PojoUtils.GetterDouble;
import org.apache.apex.malhar.lib.util.PojoUtils.GetterFloat;
import org.apache.apex.malhar.lib.util.PojoUtils.GetterInt;
import org.apache.apex.malhar.lib.util.PojoUtils.GetterLong;
import org.apache.apex.malhar.lib.util.PojoUtils.GetterShort;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import com.datatorrent.api.Context.OperatorContext;

/**
 * A generic implementation of AbstractMemsqlOutputOperator which can take in a POJO.
 *
 * @since 2.1.0
 *
 * @displayName Memsql Output Operator
 * @category Output
 * @tags database, sql, pojo, memsql
 *
 */
@Evolving
public class MemsqlPOJOOutputOperator extends AbstractMemsqlOutputOperator<Object>
{
  private static final long serialVersionUID = 20150618346L;
  @NotNull
  private String tablename;
  @NotNull
  //Columns in memsql database set by user.
  private ArrayList<String> dataColumns;
  //Expressions set by user to get field values from input tuple.
  @NotNull
  private ArrayList<String> expression;
  //These are extracted from table metadata
  private final ArrayList<Integer> columnDataTypes;
  private transient boolean isFirstTuple;
  private final transient ArrayList<Object> getters;

  /*
   * An ArrayList of Java expressions that will yield the field value from the POJO.
   * Each expression corresponds to one column in the memsql table.
   * Example:
   */
  public ArrayList<String> getExpression()
  {
    return expression;
  }

  /*
   * Set Java Expression.
   * @param ArrayList of Extraction Expressions
   */
  public void setExpression(ArrayList<String> expression)
  {
    this.expression = expression;
  }

  private String insertStatement;


  /*
   * An arraylist of data column names to be set in Memsql database.
   * Gets column names.
   */
  public ArrayList<String> getDataColumns()
  {
    return dataColumns;
  }

  /*
   * An arraylist of data column names to be set in Memsql database.
   * Sets column names.
   */
  public void setDataColumns(ArrayList<String> dataColumns)
  {
    this.dataColumns = dataColumns;
  }


  /*
   * Gets the Memsql Tablename
   */
  public String getTablename()
  {
    return tablename;
  }

  /*
   * Sets the Memsql Tablename
   */
  public void setTablename(String tablename)
  {
    this.tablename = tablename;
  }

  @Override
  public void setup(OperatorContext context)
  {
    isFirstTuple = true;
    StringBuilder columns = new StringBuilder("");
    StringBuilder values = new StringBuilder("");
    for (int i = 0; i < dataColumns.size(); i++) {
      columns.append(dataColumns.get(i));
      values.append("?");
      if (i < dataColumns.size() - 1) {
        columns.append(",");
        values.append(",");
      }
    }
    insertStatement = "INSERT INTO "
            + tablename
            + " (" + columns.toString() + ")"
            + " VALUES (" + values.toString() + ")";
    super.setup(context);
    Connection conn = store.getConnection();
    LOG.debug("Got Connection.");
    try {
      Statement st = conn.createStatement();
      ResultSet rs = st.executeQuery("select * from " + tablename);
      ResultSetMetaData rsMetaData = rs.getMetaData();

      int numberOfColumns;

      numberOfColumns = rsMetaData.getColumnCount();

      LOG.debug("resultSet MetaData column Count=" + numberOfColumns);

      for (int i = 1; i <= numberOfColumns; i++) {
        // get the designated column's SQL type.
        int type = rsMetaData.getColumnType(i);
        columnDataTypes.add(type);
        LOG.debug("sql column type is " + type);
      }
    } catch (SQLException ex) {
      throw new RuntimeException(ex);
    }

  }

  public MemsqlPOJOOutputOperator()
  {
    super();
    columnDataTypes = new ArrayList<Integer>();
    getters = new ArrayList<Object>();
  }

  @Override
  public void processTuple(Object tuple)
  {
    if (isFirstTuple) {
      processFirstTuple(tuple);
    }
    isFirstTuple = false;
    super.processTuple(tuple);
  }

  public void processFirstTuple(Object tuple)
  {
    final Class<?> fqcn = tuple.getClass();
    final int size = columnDataTypes.size();
    for (int i = 0; i < size; i++) {
      final int type = columnDataTypes.get(i);
      final String getterExpression = expression.get(i);
      final Object getter;
      switch (type) {
        case Types.CHAR:
          getter = PojoUtils.createGetterChar(fqcn, getterExpression);
          break;
        case Types.VARCHAR:
          getter = PojoUtils.createGetter(fqcn, getterExpression, String.class);
          break;
        case Types.BOOLEAN:
        case Types.TINYINT:
          getter = PojoUtils.createGetterBoolean(fqcn, getterExpression);
          break;
        case Types.SMALLINT:
          getter = PojoUtils.createGetterShort(fqcn, getterExpression);
          break;
        case Types.INTEGER:
          getter = PojoUtils.createGetterInt(fqcn, getterExpression);
          break;
        case Types.BIGINT:
          getter = PojoUtils.createGetterLong(fqcn, getterExpression);
          break;
        case Types.FLOAT:
          getter = PojoUtils.createGetterFloat(fqcn, getterExpression);
          break;
        case Types.DOUBLE:
          getter = PojoUtils.createGetterDouble(fqcn, getterExpression);
          break;
        default:
          /*
            Types.DECIMAL
            Types.DATE
            Types.TIME
            Types.ARRAY
            Types.OTHER
           */
          getter = PojoUtils.createGetter(fqcn, getterExpression, Object.class);
          break;
      }
      getters.add(getter);
    }

  }

  @Override
  protected String getUpdateCommand()
  {
    LOG.debug("insertstatement is {}", insertStatement);
    return insertStatement;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected void setStatementParameters(PreparedStatement statement, Object tuple) throws SQLException
  {
    final int size = columnDataTypes.size();
    for (int i = 0; i < size; i++) {
      final int type = columnDataTypes.get(i);
      switch (type) {
        case (Types.CHAR):
          // TODO: verify that memsql driver handles char as int
          statement.setInt(i + 1, ((GetterChar<Object>)getters.get(i)).get(tuple));
          break;
        case (Types.VARCHAR):
          statement.setString(i + 1, ((Getter<Object, String>)getters.get(i)).get(tuple));
          break;
        case (Types.BOOLEAN):
        case (Types.TINYINT):
          statement.setBoolean(i + 1, ((GetterBoolean<Object>)getters.get(i)).get(tuple));
          break;
        case (Types.SMALLINT):
          statement.setShort(i + 1, ((GetterShort<Object>)getters.get(i)).get(tuple));
          break;
        case (Types.INTEGER):
          statement.setInt(i + 1, ((GetterInt<Object>)getters.get(i)).get(tuple));
          break;
        case (Types.BIGINT):
          statement.setLong(i + 1, ((GetterLong<Object>)getters.get(i)).get(tuple));
          break;
        case (Types.FLOAT):
          statement.setFloat(i + 1, ((GetterFloat<Object>)getters.get(i)).get(tuple));
          break;
        case (Types.DOUBLE):
          statement.setDouble(i + 1, ((GetterDouble<Object>)getters.get(i)).get(tuple));
          break;
        default:
          /*
            Types.DECIMAL
            Types.DATE
            Types.TIME
            Types.ARRAY
            Types.OTHER
           */
          statement.setObject(i + 1, ((Getter<Object, Object>)getters.get(i)).get(tuple));
          break;
      }
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(MemsqlPOJOOutputOperator.class);

}
