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
package com.datatorrent.contrib.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datatorrent.contrib.memsql.MemsqlOutputOperator;
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

/**
 * <p>
 * CassandraOutputOperator class.</p>
 * A Generic implementation of AbstractCassandraTransactionableOutputOperatorPS which takes in any POJO.
 *
 * @since 1.0.3
 */
public class CassandraOutputOperator extends AbstractCassandraTransactionableOutputOperatorPS<Object>
{
  private ArrayList<String> columns;
  private ArrayList<String> columnDataTypes;
  private ArrayList<String> expressions;
  private transient ArrayList<Object> getters;

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
  private String keyspace;

  /*
   * The Cassandra keyspace is a namespace that defines how data is replicated on nodes.
   * Typically, a cluster has one keyspace per application. Replication is controlled on a per-keyspace basis, so data that has different replication requirements typically resides in different keyspaces.
   * Keyspaces are not designed to be used as a significant map layer within the data model. Keyspaces are designed to control data replication for a set of tables.
   */
  public String getKeyspace()
  {
    return keyspace;
  }

  public void setKeyspace(String keyspace)
  {
    this.keyspace = keyspace;
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

  public CassandraOutputOperator()
  {
    super();
    columnDataTypes = new ArrayList<String>();
    getters = new ArrayList<Object>();
  }


  @Override
  public void processTuple(Object tuple)
  {
    if (getters.isEmpty()) {
      processFirstTuple(tuple);
    }
    super.processTuple(tuple);
  }

  public void processFirstTuple(Object tuple)
  {
    Class<?> fqcn = tuple.getClass();
    int size = columnDataTypes.size();
    for (int i = 0; i < size; i++) {
      String type = columnDataTypes.get(i);
      String getterExpression = expressions.get(i);
     /* if (type == Types.CHAR) {
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
        GetterLong getLong = PojoUtils.createExpressionGetterLong(fqcn, getterExpression);
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
      }*/

    }

  }

  @Override
  protected PreparedStatement getUpdateCommand()
  {

      com.datastax.driver.core.ResultSet rs = store.getSession().execute("select * from " + tablename);

      ColumnDefinitions rsMetaData = rs.getColumnDefinitions();

      int numberOfColumns = 0;

      numberOfColumns = rsMetaData.size();

      LOG.debug("resultSet MetaData column Count=" + numberOfColumns);

      for (int i = 1; i <= numberOfColumns; i++) {
        // get the designated column's SQL type.
        String type = rsMetaData.getName(i);
        columnDataTypes.add(type);
        LOG.debug("sql column type is " + type);
      }

    StringBuilder queryfields = new StringBuilder("");
    StringBuilder values = new StringBuilder();
    for (int i = 0; i < columns.size(); i++) {
      if (queryfields.equals("")) {
        queryfields.append(columns.get(i));
        values.append("?");
      }
      else {
        queryfields.append(",").append(columns.get(i));
        values.append(",").append(columns.get(i));
      }
    }
    String statement
            = "INSERT INTO " + keyspace + "."
            + tablename
            + " (" + queryfields.toString() + ")"
            + " VALUES (" + values.toString() + ")";
    return store.getSession().prepare(statement);
  }

  @Override
  protected Statement setStatementParameters(PreparedStatement updateCommand, Object tuple) throws DriverException
  {
    BoundStatement boundStmnt = new BoundStatement(updateCommand);
    int size = columnDataTypes.size();
    Object getter;
    for (int i = 0; i < size; i++) {
      String type = columnDataTypes.get(i);
      getter = ((GetterInt)getters.get(i)).get(tuple);
     /* switch (type) {
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
      }*/
      boundStmnt.bind(i + 1, getter);

    }
    return boundStmnt;
  }

  private static transient final Logger LOG = LoggerFactory.getLogger(CassandraOutputOperator.class);
}
