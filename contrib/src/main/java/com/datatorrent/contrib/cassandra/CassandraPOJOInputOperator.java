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

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Setter;
import com.datatorrent.lib.util.PojoUtils.SetterBoolean;
import com.datatorrent.lib.util.PojoUtils.SetterDouble;
import com.datatorrent.lib.util.PojoUtils.SetterFloat;
import com.datatorrent.lib.util.PojoUtils.SetterInt;
import com.datatorrent.lib.util.PojoUtils.SetterLong;
import java.math.BigDecimal;
import java.util.*;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * CassandraPOJOInputOperator</p>
 * A Generic implementation of AbstractCassandraInputOperator which gets field values from Cassandra database columns and sets in a POJO.
 * User can give a parameterized query with parameters %t for table name, %p for primary key, %s for start value and %l for limit.
 *
 * @displayName Cassandra POJO Input Operator
 * @category Input
 * @tags input operator
 */
public class CassandraPOJOInputOperator extends AbstractCassandraInputOperator<Object>
{
  @NotNull
  private List<String> columns;
  private final transient List<DataType> columnDataTypes;
  private Number startRow = 0;
  @NotNull
  private List<String> expressions;
  @NotNull
  private String tablename;
  private final transient List<Object> setters;
  @NotNull
  private String query;

  private transient Class<?> objectClass = null;
  @NotNull
  protected String primaryKeyColumn;
  protected transient DataType primaryKeyColumnType;
  private transient Row lastRowInBatch;

  @Min(1)
  private int limit = 10;

  /*
   * Number of records to be fetched in one time from cassandra table.
   */
  public int getLimit()
  {
    return limit;
  }

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
   * select * from %t where token(%p) > %s limit %l;
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
   * List of column names specified by User in the same order as expressions for the particular fields.
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
    if (setters.isEmpty()) {
      try {
        // This code will be replaced after integration of creating POJOs on the fly utility.
        objectClass = Class.forName(outputClass);
      }
      catch (ClassNotFoundException ex) {
        throw new RuntimeException(ex);
      }

      com.datastax.driver.core.ResultSet rs = store.getSession().execute("select * from " + store.keyspace + "." + tablename + " LIMIT " + 1);
      ColumnDefinitions rsMetaData = rs.getColumnDefinitions();
      int numberOfColumns = rsMetaData.size();

      primaryKeyColumnType = rsMetaData.getType(primaryKeyColumn);
       if(query.contains("%p"))
       {
          query = query.replace("%p", primaryKeyColumn);
       }
       if(query.contains("%l"))
       {
         query = query.replace("%l", limit+"");
       }

      logger.debug("query is {}",query);

      //In case columns is a subset
      int columnSize = columns.size();
      if (columns.size()<numberOfColumns){
        numberOfColumns = columnSize;
      }
      for (int i = 0; i < numberOfColumns; i++) {
        // Get the designated column's data type.
        DataType type = rsMetaData.getType(columns.get(i));
        columnDataTypes.add(type);
        Object setter;
        final String setterExpr = expressions.get(i);
        switch (type.getName()) {
          case ASCII:
          case TEXT:
          case VARCHAR:
            setter = PojoUtils.createSetter(objectClass, setterExpr, String.class);
            break;
          case BOOLEAN:
            setter = PojoUtils.createSetterBoolean(objectClass, setterExpr);
            break;
          case INT:
            setter = PojoUtils.createSetterInt(objectClass, setterExpr);
            break;
          case BIGINT:
          case COUNTER:
            setter = PojoUtils.createSetterLong(objectClass, setterExpr);
            break;
          case FLOAT:
            setter = PojoUtils.createSetterFloat(objectClass, setterExpr);
            break;
          case DOUBLE:
            setter = PojoUtils.createSetterDouble(objectClass, setterExpr);
            break;
          case DECIMAL:
            setter = PojoUtils.createSetter(objectClass, setterExpr, BigDecimal.class);
            break;
          case SET:
            setter = PojoUtils.createSetter(objectClass, setterExpr, Set.class);
            break;
          case MAP:
            setter = PojoUtils.createSetter(objectClass, setterExpr, Map.class);
            break;
          case LIST:
            setter = PojoUtils.createSetter(objectClass, setterExpr, List.class);
            break;
          case TIMESTAMP:
            setter = PojoUtils.createSetter(objectClass, setterExpr, Date.class);
            break;
          case UUID:
            setter = PojoUtils.createSetter(objectClass, setterExpr, UUID.class);
            break;
          default:
            setter = PojoUtils.createSetter(objectClass, setterExpr, Object.class);
            break;
        }
        setters.add(setter);
      }
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object getTuple(Row row)
  {
    lastRowInBatch = row;
    Object obj = null;
    final int size = columnDataTypes.size();

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

    for (int i = 0; i < size; i++) {
      DataType type = columnDataTypes.get(i);
      String columnName = columns.get(i);
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
          final Date date = row.getDate(columnName);
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
    String parameterizedQuery;
    if(query.contains("%v"))
    {
      parameterizedQuery = query.replace("%v", startRow+"");
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
    if (lastRowInBatch != null) {
      switch (primaryKeyColumnType.getName()) {
        case INT:
          startRow = lastRowInBatch.getInt(primaryKeyColumn);
          break;
        case COUNTER:
          startRow = lastRowInBatch.getLong(primaryKeyColumn);
          break;
        case FLOAT:
          startRow = lastRowInBatch.getFloat(primaryKeyColumn);
          break;
        case DOUBLE:
          startRow = lastRowInBatch.getDouble(primaryKeyColumn);
          break;
        default:
          throw new RuntimeException("unsupported data type " + primaryKeyColumnType.getName());
      }
    }

  }

  private static final Logger logger = LoggerFactory.getLogger(CassandraPOJOInputOperator.class);
}
