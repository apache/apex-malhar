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
import com.datatorrent.lib.util.PojoUtils.Setter;
import com.datatorrent.lib.util.PojoUtils.SetterBoolean;
import com.datatorrent.lib.util.PojoUtils.SetterDouble;
import com.datatorrent.lib.util.PojoUtils.SetterFloat;
import com.datatorrent.lib.util.PojoUtils.SetterInt;
import com.datatorrent.lib.util.PojoUtils.SetterLong;
import com.datatorrent.lib.util.PojoUtils.SetterShort;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * MemsqlPOJOInputOperator</p>
 * A Generic implementation of AbstractMemsqlInputOperator which gets field values from memsql database columns and sets in a POJO.
 * User can give a parameterized query with parameters %t for table name, %p for primary key, %s for start value and %l for limit.
 * @displayName Memsql POJO Input Operator
 * @category Input
 * @tags input operator
 */
public class MemsqlPOJOInputOperator extends AbstractMemsqlInputOperator<Object>
{
  private final transient List<Integer> columnDataTypes;
  @Min(1)
  private int limit = 10;
  @Min(1)
  private Number startRow = 1;
  @NotNull
  private List<String> expressions;
  @NotNull
  private String tablename;
  private transient List<Object> setters;
  private transient Class<?> objectClass = null;
  @NotNull
  private String primaryKeyColumn;
  @NotNull
  private List<String> columns;
  private boolean useAllColumns;
  private transient Number lastRowKey = 1;
  private transient int primaryKeyColumnType;
  @NotNull
  private String query;

  /*
   * Set of columns specified by User in case POJO needs to contain fields specific to these columns only.
   * User should specify columns in same order as expressions for fields.
   */
  public List<String> getColumns()
  {
    return columns;
  }

  public void setColumns(List<String> columns)
  {
    this.columns = columns;
  }

  /*
   * Primary Key Column of table.
   * Gets the primary key column of Memsql table.
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
   * User has the option to specify the start row.
   */
  public Number getStartRow()
  {
    return startRow;
  }

  public void setStartRow(Number startRow)
  {
    this.startRow = startRow;
  }

  public void setLimit(int limit)
  {
    this.limit = limit;
  }

  /*
   * Records are read in batches of this size.
   * Gets the batch size.
   * @return batchsize
   */
  public int getLimit()
  {
    return limit;
  }

  /*
   * This option is for user to create POJO fields from a subset of columns in cassandra table.
   */
  public boolean isUseAllColumns()
  {
    return useAllColumns;
  }

  public void setUseAllColumns(boolean useAllColumns)
  {
    this.useAllColumns = useAllColumns;
  }


  /*
   * POJO class which is generated as output from this operator.
   * Example:
   * public class TestPOJO{ int intfield; public int getInt(){} public void setInt(){} }
   * outputClass = TestPOJO
   * POJOs will be generated on fly in later implementation.
   */
  private String outputClass;

  public String getOutputClass()
  {
    return outputClass;
  }

  public void setOutputClass(String outputClass)
  {
    this.outputClass = outputClass;
  }

  /*
   * Parameterized query with parameters such as %t for table name , %p for primary key, %s for start value and %l for limit.
   * Example of retrieveQuery:
   * select * from %t where %p > %s and %p < %e;
   */
  public String getQuery()
  {
    return query;
  }

  public void setQuery(String query)
  {
    this.query = query.replace("%t", tablename);
  }

  /*
   * An ArrayList of Java expressions that will yield the cassandra column value to be set in output object.
   * Each expression corresponds to one column in the Cassandra table.
   */
  public List<String> getExpressions()
  {
    return expressions;
  }

  public void setExpressions(List<String> expressions)
  {
    this.expressions = expressions;
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

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    int numberOfColumns = 0;
    ResultSet resultSet;
    ResultSetMetaData rsMetaData;
    try {
      Statement statement = store.getConnection().createStatement();
      resultSet = statement.executeQuery("describe " + tablename);
      rsMetaData = resultSet.getMetaData();
      primaryKeyColumnType = rsMetaData.getColumnType(resultSet.findColumn(primaryKeyColumn));
      if(query.contains("%p"))
      {
          query = query.replace("%p", primaryKeyColumn);
      }
      numberOfColumns = rsMetaData.getColumnCount();

      logger.debug("column Count=" + numberOfColumns);
      statement.close();
    }
    catch (SQLException ex) {
      throw new RuntimeException(ex);
    }

    setters = new ArrayList<Object>();
    try {
      // This code will be replaced after integration of creating POJOs on the fly utility.
      objectClass = Class.forName(outputClass);
    }
    catch (ClassNotFoundException ex) {
      throw new RuntimeException(ex);
    }

    for (int i = 1; i <= numberOfColumns; i++) {
      int type = 0;
      try {
        type = rsMetaData.getColumnType(i);
        columnDataTypes.add(type);
      }
      catch (SQLException ex) {
        throw new RuntimeException(ex);
      }

      logger.debug("memsql column type is " + type);

      final String setterExpression = expressions.get(i - 1);
      final Object setter;
      switch (type) {
        case Types.CHAR:
          setter = PojoUtils.createSetterChar(objectClass, setterExpression);
          break;
        case Types.VARCHAR:
          setter = PojoUtils.createSetter(objectClass, setterExpression, String.class);
          break;
        case Types.BOOLEAN:
        case Types.TINYINT:
          setter = PojoUtils.createSetterBoolean(objectClass, setterExpression);
          break;
        case Types.SMALLINT:
          setter = PojoUtils.createSetterShort(objectClass, setterExpression);
          break;
        case Types.INTEGER:
          setter = PojoUtils.createSetterInt(objectClass, setterExpression);
          break;
        case Types.BIGINT:
          setter = PojoUtils.createSetterLong(objectClass, setterExpression);
          break;
        case Types.FLOAT:
          setter = PojoUtils.createSetterFloat(objectClass, setterExpression);
          break;
        case Types.DOUBLE:
          setter = PojoUtils.createSetterDouble(objectClass, setterExpression);
          break;
        default:
          /*
           Types.DECIMAL
           Types.DATE
           Types.TIME
           Types.ARRAY
           Types.OTHER
           */
          setter = PojoUtils.createSetter(objectClass, setterExpression, Object.class);
          break;
      }
      setters.add(setter);
    }

  }

  public MemsqlPOJOInputOperator()
  {
    super();
    columnDataTypes = new ArrayList<Integer>();
    setters = new ArrayList<Object>();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object getTuple(ResultSet result)
  {
    Object obj;
    try {
      // This code will be replaced after integration of creating POJOs on the fly utility.
      obj = objectClass.newInstance();
    }
    catch (InstantiationException ex) {
      throw new RuntimeException(ex);
    }
    catch (IllegalAccessException ex) {
      throw new RuntimeException(ex);
    }
    final int size = columnDataTypes.size();
    for (int i = 0; i < size; i++) {
      String columnName = columns.get(i);
      final int type = columnDataTypes.get(i);
      try {
        switch (type) {
          case (Types.CHAR):
          case (Types.VARCHAR):
            ((Setter<Object, String>)setters.get(i)).set(obj, result.getString(columnName));
            break;
          case (Types.BOOLEAN):
          case (Types.TINYINT):
            ((SetterBoolean)setters.get(i)).set(obj, result.getBoolean(columnName));
            break;
          case (Types.SMALLINT):
            ((SetterShort)setters.get(i)).set(obj, result.getShort(columnName));
            break;
          case (Types.INTEGER):
            ((SetterInt)setters.get(i)).set(obj, result.getInt(columnName));
            break;
          case (Types.BIGINT):
            ((SetterLong)setters.get(i)).set(obj, result.getLong(columnName));
            break;
          case (Types.FLOAT):
            ((SetterFloat)setters.get(i)).set(obj, result.getFloat(columnName));
            break;
          case (Types.DOUBLE):
            ((SetterDouble)setters.get(i)).set(obj, result.getDouble(columnName));
            break;
          case (Types.DECIMAL):
            ((Setter<Object, BigDecimal>)setters.get(i)).set(obj, result.getBigDecimal(columnName));
            break;
          case (Types.DATE):
            ((Setter<Object, Date>)setters.get(i)).set(obj, result.getDate(columnName));
            break;
          case (Types.TIMESTAMP):
            ((Setter<Object, Timestamp>)setters.get(i)).set(obj, result.getTimestamp(columnName));
            break;
          case (Types.OTHER):
            ((Setter<Object, Object>)setters.get(i)).set(obj, result.getObject(columnName));
            break;
          default:
            throw new RuntimeException("unsupported data type " + type);
        }
      }
      catch (SQLException ex) {
        throw new RuntimeException(ex);
      }
    }
    try {
      if (result.last()) {
        switch (primaryKeyColumnType) {
          case Types.INTEGER:
            lastRowKey = result.getInt(primaryKeyColumn);
            break;
          case Types.BIGINT:
            lastRowKey = result.getLong(primaryKeyColumn);
            break;
          case Types.FLOAT:
            lastRowKey = result.getFloat(primaryKeyColumn);
            break;
          case Types.DOUBLE:
            lastRowKey = result.getDouble(primaryKeyColumn);
            break;
          case Types.SMALLINT:
            lastRowKey = result.getShort(primaryKeyColumn);
            break;
          default:
            throw new RuntimeException("unsupported data type " + primaryKeyColumnType);
        }
      }
    }

    catch (SQLException ex) {
      throw new RuntimeException(ex);
    }
    return obj;
  }

  /*
   * This method replaces the parameters in Query with actual values given by user.
   * Example of retrieveQuery:
   * select * from %t where %p > %s and %p < %e;
   */
  @Override
  public String queryToRetrieveData()
  {
    String parameterizedQuery;
    int endRow = startRow.intValue() + limit;

    if(query.contains("%s"))
    {
      parameterizedQuery = query.replace("%v", startRow+"");
    }
    else if(query.contains("%e"))
    {
      parameterizedQuery = query.replace("%e", endRow+"");
    }
    else
    {
      parameterizedQuery = query;
    }
    return parameterizedQuery;
  }


  /*
   * Overriding emitTupes to save primarykey column value from last row in batch.
   */
  @Override
  public void emitTuples()
  {
    super.emitTuples();
    startRow = lastRowKey;
  }

  private static final Logger logger = LoggerFactory.getLogger(MemsqlPOJOInputOperator.class);

}
