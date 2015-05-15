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
package com.datatorrent.contrib.memsql;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.GetterBoolean;
import com.datatorrent.lib.util.PojoUtils.GetterChar;
import com.datatorrent.lib.util.PojoUtils.GetterDouble;
import com.datatorrent.lib.util.PojoUtils.GetterFloat;
import com.datatorrent.lib.util.PojoUtils.GetterInt;
import com.datatorrent.lib.util.PojoUtils.GetterLong;
import com.datatorrent.lib.util.PojoUtils.GetterObject;
import com.datatorrent.lib.util.PojoUtils.GetterShort;
import com.datatorrent.lib.util.PojoUtils.GetterString;
import java.sql.*;
import java.util.ArrayList;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * A generic implementation of AbstractMemsqlOutputOperator which can take in a POJO.
 */
public class MemsqlOutputOperator extends AbstractMemsqlOutputOperator<Object>
{
  @NotNull
  private String tablename;
  @NotNull
  //Columns in memsql database set by user.
  private ArrayList<String> dataColumns;
  //Expressions set by user to get field values from input tuple.
  @NotNull
  private ArrayList<String> expression;
  //These are extracted from table metadata
  private ArrayList<Integer> columnDataTypes;
  private transient boolean isFirstTuple;
  private transient ArrayList<Object> getters;

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

      int numberOfColumns = 0;

      numberOfColumns = rsMetaData.getColumnCount();

      LOG.debug("resultSet MetaData column Count=" + numberOfColumns);

      for (int i = 1; i <= numberOfColumns; i++) {
        // get the designated column's SQL type.
        int type = rsMetaData.getColumnType(i);
        columnDataTypes.add(type);
        LOG.debug("sql column type is " + type);
      }
    }
    catch (SQLException ex) {
      throw new RuntimeException(ex);
    }

  }

  public MemsqlOutputOperator()
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
    Class<?> fqcn = tuple.getClass();
    int size = columnDataTypes.size();
    for (int i = 0; i < size; i++) {
      int type = columnDataTypes.get(i);
      String getterExpression = expression.get(i);
      if (type == Types.CHAR) {
        GetterChar getChar = PojoUtils.createGetterChar(fqcn, getterExpression);
        getters.add(getChar);
      }
      else if (type == Types.VARCHAR) {
        GetterString getVarchar = PojoUtils.createGetterString(fqcn, getterExpression);
        getters.add(getVarchar);
      }
      else if (type == Types.BOOLEAN || type == Types.TINYINT) {
        GetterBoolean getBoolean = PojoUtils.createGetterBoolean(fqcn, getterExpression);
        getters.add(getBoolean);
      }
      else if (type == Types.SMALLINT) {
        GetterShort getShort = PojoUtils.createGetterShort(fqcn, getterExpression);
        getters.add(getShort);
      }
      else if (type == Types.INTEGER) {
        GetterInt getInt = PojoUtils.createGetterInt(fqcn, getterExpression);
        getters.add(getInt);
      }
      else if (type == Types.BIGINT) {
        GetterLong getLong = PojoUtils.createGetterLong(fqcn, getterExpression);
        getters.add(getLong);
      }
      else if (type == Types.DECIMAL) {
        GetterObject getObject = PojoUtils.createGetterObject(fqcn, getterExpression);
        getters.add(getObject);
      }
      else if (type == Types.FLOAT) {
        GetterFloat getFloat = PojoUtils.createGetterFloat(fqcn, getterExpression);
        getters.add(getFloat);
      }
      else if (type == Types.DOUBLE) {
        GetterDouble getDouble = PojoUtils.createGetterDouble(fqcn, getterExpression);
        getters.add(getDouble);
      }
      else if (type == Types.DATE) {
        GetterObject getObject = PojoUtils.createGetterObject(fqcn, getterExpression);
        getters.add(getObject);
      }
      else if (type == Types.TIME) {
        GetterObject getObject = PojoUtils.createGetterObject(fqcn, getterExpression);
        getters.add(getObject);
      }
      else if (type == Types.ARRAY) {
        GetterObject getObject = PojoUtils.createGetterObject(fqcn, getterExpression);
        getters.add(getObject);
      }
      else if (type == Types.OTHER) {
        GetterObject getObject = PojoUtils.createGetterObject(fqcn, getterExpression);
        getters.add(getObject);
      }

    }

  }

  @Override
  protected String getUpdateCommand()
  {
    LOG.debug("insertstatement is {}", insertStatement);
    return insertStatement;
  }

  @Override
  protected void setStatementParameters(PreparedStatement statement, Object tuple) throws SQLException
  {
    int size = columnDataTypes.size();
    Object getter;
    for (int i = 0; i < size; i++) {
      int type = columnDataTypes.get(i);
      switch (type) {
        case (Types.CHAR):
          getter = ((GetterChar)getters.get(i)).get(tuple);
          break;
        case (Types.VARCHAR):
          getter = ((GetterString)getters.get(i)).get(tuple);
          break;
        case (Types.BOOLEAN):
          getter = ((GetterBoolean)getters.get(i)).get(tuple);
          break;
        case (Types.SMALLINT):
          getter = ((GetterShort)getters.get(i)).get(tuple);
          break;
        case (Types.INTEGER):
          getter = ((GetterInt)getters.get(i)).get(tuple);
          break;
        case (Types.BIGINT):
          getter = ((GetterLong)getters.get(i)).get(tuple);
          break;
        case (Types.DECIMAL):
          getter = (Number)((GetterObject)getters.get(i)).get(tuple);
          break;
        case (Types.FLOAT):
          getter = ((GetterFloat)getters.get(i)).get(tuple);
          break;
        case (Types.DOUBLE):
          getter = ((GetterDouble)getters.get(i)).get(tuple);
          break;
        case (Types.DATE):
          getter = (Date)((GetterObject)getters.get(i)).get(tuple);
          break;
        case (Types.TIME):
          getter = (Timestamp)((GetterObject)getters.get(i)).get(tuple);
          break;
        case (Types.ARRAY):
          getter = (Array)((GetterObject)getters.get(i)).get(tuple);
          break;
        case (Types.OTHER):
          getter = ((GetterObject)getters.get(i)).get(tuple);
          break;
        default:
          getter = ((GetterObject)getters.get(i)).get(tuple);
          break;
      }
      statement.setObject(i + 1, getter);
    }
  }

  private static transient final Logger LOG = LoggerFactory.getLogger(MemsqlOutputOperator.class);

}
