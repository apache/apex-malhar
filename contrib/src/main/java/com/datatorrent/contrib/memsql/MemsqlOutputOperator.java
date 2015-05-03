/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
import com.datatorrent.lib.util.ConvertUtils;
import com.datatorrent.lib.util.ConvertUtils.GetterBoolean;
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
  private ArrayList<String> dataColumns;
  private ArrayList<String> expression;
  //private ArrayList<String> columnDataTypes;

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
            + " (" + dataColumns + ")"
            + " values (" + values + ")";
    super.setup(context);
    Connection conn = store.getConnection();
    System.out.println("Got Connection.");
    try {
      Statement st = conn.createStatement();
      ResultSet rs = st.executeQuery("select * from" + tablename);

      ResultSetMetaData rsMetaData = rs.getMetaData();

      int numberOfColumns = 0;

      numberOfColumns = rsMetaData.getColumnCount();

      System.out.println("resultSet MetaData column Count=" + numberOfColumns);

      for (int i = 1; i <= numberOfColumns; i++) {
        // get the designated column's SQL type.
        //columnDataTypes.add(rsMetaData.getColumnName(i));
        int type = rsMetaData.getColumnType(i);
        LOG.debug("sql column name is " + type);

        if(type == Types.CHAR)
        {
          GetterBoolean getBoolean = ConvertUtils.createExpressionGetterBoolean(fqcn, "innerObj.boolVal");
        }
        else if(type == Types.VARCHAR)
        {

        }
        else if(type == Types.BINARY)
        {

        }
        else if(type == Types.INTEGER)
        {

        }
        else if(type == Types.BIGINT)
        {

        }
        else if(type == Types.DECIMAL)
        {

        }
        else if(type == Types.FLOAT)
        {

        }
        else if(type == Types.REAL)
        {

        }
        else if(type == Types.DOUBLE)
        {

        }
        else if(type == Types.DATE)
        {

        }
        else if(type == Types.TIME)
        {

        }
        else if(type == Types.TIMESTAMP)
        {

        }
        else if(type == Types.ARRAY)
        {

        }
        else if(type == Types.OTHER)
        {

        }

      }
    }
    catch (SQLException ex) {
      throw new RuntimeException(ex);
    }


  }

  public MemsqlOutputOperator()
  {
  }

  @Override
  protected String getUpdateCommand()
  {
    return insertStatement;
  }

  @Override
  protected void setStatementParameters(PreparedStatement statement, Object tuple) throws SQLException
  {
    statement.setObject(1, tuple);
  }

  private static transient final Logger LOG = LoggerFactory.getLogger(MemsqlOutputOperator.class);

}
