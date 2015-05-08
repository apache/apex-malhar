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
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.GetterBoolean;
import com.datatorrent.lib.util.PojoUtils.GetterDouble;
import com.datatorrent.lib.util.PojoUtils.GetterFloat;
import com.datatorrent.lib.util.PojoUtils.GetterInt;
import com.datatorrent.lib.util.PojoUtils.GetterLong;
import com.datatorrent.lib.util.PojoUtils.GetterObject;
import com.datatorrent.lib.util.PojoUtils.GetterString;
import java.util.ArrayList;
import java.util.Date;
import java.util.UUID;
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
  @NotNull
  private ArrayList<String> columns;
  private ArrayList<DataType> columnDataTypes;
  @NotNull
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
    columnDataTypes = new ArrayList<DataType>();
    getters = new ArrayList<Object>();
  }

  public void processFirstTuple(Object tuple)
  {
     com.datastax.driver.core.ResultSet rs = store.getSession().execute("select * from " + store.keyspace +"."+tablename);

      ColumnDefinitions rsMetaData = rs.getColumnDefinitions();

      int numberOfColumns = 0;

      numberOfColumns = rsMetaData.size();
      for (int i = 0; i < numberOfColumns; i++) {
        // get the designated column's data type.
        DataType type = rsMetaData.getType(i);
        columnDataTypes.add(type);
      }
    Class<?> fqcn = tuple.getClass();
    int size = columnDataTypes.size();
    for (int i = 0; i < size; i++) {
      DataType type = columnDataTypes.get(i);
      LOG.debug("type is {}",type.getName());
      String getterExpression = PojoUtils.getSingleFieldExpression(fqcn, expressions.get(i));
      if (type.equals(DataType.ascii()) || type.equals(DataType.text()) || type.equals(DataType.varchar())) {
        GetterString getVarchar = PojoUtils.createGetterString(fqcn, getterExpression);
        getters.add(getVarchar);
      }
       else if (type.equals(DataType.uuid())) {
        GetterObject getObject = PojoUtils.createGetterObject(fqcn, getterExpression);
        getters.add(getObject);
      }
      else if (type.equals(DataType.cboolean())) {
        GetterBoolean getBoolean = PojoUtils.createGetterBoolean(fqcn, getterExpression);
        getters.add(getBoolean);
      }
      else if (type.equals(DataType.cint())) {
        GetterInt getInt = PojoUtils.createGetterInt(fqcn, getterExpression);
        getters.add(getInt);
      }
      else if (type.equals(DataType.bigint()) || type.equals(DataType.counter())) {
        GetterLong getLong = PojoUtils.createExpressionGetterLong(fqcn, getterExpression);
        getters.add(getLong);
      }
      else if (type.equals(DataType.cfloat())) {
        GetterFloat getFloat = PojoUtils.createGetterFloat(fqcn, getterExpression);
        getters.add(getFloat);
      }
      else if (type.equals(DataType.cdouble())) {
        GetterDouble getDouble = PojoUtils.createGetterDouble(fqcn, getterExpression);
        getters.add(getDouble);
      }
      else if (type.equals(DataType.timestamp())) {
        GetterObject getObject = PojoUtils.createGetterObject(fqcn, getterExpression);
        getters.add(getObject);
      }
      else
      {
        throw new UnsupportedOperationException("this type is not supported "+type);
      }

    }

  }

  @Override
  protected PreparedStatement getUpdateCommand()
  {
    StringBuilder queryfields = new StringBuilder("");
    StringBuilder values = new StringBuilder("");
    for (int i = 0; i < columns.size(); i++) {
      if (queryfields.length()==0) {
        queryfields.append(columns.get(i));
        values.append("?");
      }
      else {
        queryfields.append(",").append(columns.get(i));
        values.append(",").append("?");
      }
    }
    LOG.debug("queryfields are", queryfields.toString());
    LOG.debug("values are ",values.toString());
    String statement
            = "INSERT INTO " + store.keyspace + "."
            + tablename
            + " (" + queryfields.toString() + ") "
            + "VALUES (" + values.toString() + ");";
    LOG.debug("statement is {}", statement);

    return store.getSession().prepare(statement);
  }

  @Override
  protected Statement setStatementParameters(PreparedStatement updateCommand, Object tuple) throws DriverException
  {
    if (getters.isEmpty()) {
      processFirstTuple(tuple);
    }
    BoundStatement boundStmnt = new BoundStatement(updateCommand);
    int size = columnDataTypes.size();
    Object getter = new Object();
    UUID id = (UUID)(((GetterObject)getters.get(0)).get(tuple));;
    for (int i = 1; i < size; i++) {
      DataType type = columnDataTypes.get(i);
      LOG.debug("type before switch is {}",type.getName());
       switch (type.getName()) {
        case UUID:
          id = (UUID)(((GetterObject)getters.get(i)).get(tuple));
          break;
        case ASCII:
         getter = ((GetterString)getters.get(i)).get(tuple);
          break;
        case VARCHAR:
          getter = ((GetterString)getters.get(i)).get(tuple);
          break;
        case TEXT:
          getter = ((GetterString)getters.get(i)).get(tuple);
          break;
        case BOOLEAN:
          getter = ((GetterBoolean)getters.get(i)).get(tuple);
          break;
        case INT:
          getter = ((GetterInt)getters.get(i)).get(tuple);
          break;
        case BIGINT:
          getter = ((GetterLong)getters.get(i)).get(tuple);
          break;
        case COUNTER:
          getter = ((GetterLong)getters.get(i)).get(tuple);
          break;
        case FLOAT:
          getter = ((GetterFloat)getters.get(i)).get(tuple);
          break;
        case DOUBLE:
          getter = ((GetterDouble)getters.get(i)).get(tuple);
          break;
        case TIMESTAMP:
          getter = (Date)((GetterObject)getters.get(i)).get(tuple);
          break;
        case CUSTOM:
          getter = ((GetterObject)getters.get(i)).get(tuple);
          break;
        default:
          getter = (((GetterObject)getters.get(i)).get(tuple));
          break;
      }
       /*if(i==0)
       {
         id = getter;
       }*/
        boundStmnt.bind(id,getter);
    }

    return boundStmnt;
  }

  private static transient final Logger LOG = LoggerFactory.getLogger(CassandraOutputOperator.class);
}

