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
import com.datastax.driver.core.ResultSet;
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
 * CassandraInputOperator</p>
 * A Generic implementation of AbstractCassandraInputOperator which gets field values from Cassandra database columns and sets in a POJO.
 *
 * @displayName Cassandra POJO Input Operator
 * @category Input
 * @tags input operator
 */
public class CassandraPOJOInputOperator extends AbstractCassandraInputOperator<Object>
{
  private List<String> columns;
  private final transient List<DataType> columnDataTypes;
  private Number startRow = 0;
  @NotNull
  private List<String> expressions;
  @NotNull
  private String tablename;
  private final transient List<Object> setters;
  @NotNull
  private String retrieveQuery;
  private transient Class<?> objectClass = null;
  private boolean useAllColumns;
  protected Number lastRowIdInBatch = 0;
  @NotNull
  protected String primaryKeyColumn;
  protected transient DataType primaryKeyColumnType;

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
   * Query input by user: Example: select * from keyspace.tablename;
   */
  public String getRetrieveQuery()
  {
    return retrieveQuery;
  }

  public void setRetrieveQuery(String retrieveQuery)
  {
    this.retrieveQuery = retrieveQuery;
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
   * Subset of columns specified by User in case POJO needs to contain fields specific to these columns only.
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

      int numberOfColumns;

      com.datastax.driver.core.ResultSet rs = store.getSession().execute("select * from " + store.keyspace + "." + tablename + " LIMIT " + 1);
      ColumnDefinitions rsMetaData = rs.getColumnDefinitions();
      if (!isUseAllColumns()) {
        numberOfColumns = rsMetaData.size();
      }
      else {
        numberOfColumns = columns.size();
      }

      primaryKeyColumnType = rsMetaData.getType(primaryKeyColumn);

      for (int i = 0; i < numberOfColumns; i++) {
        // Get the designated column's data type.
        DataType type;
        if (!isUseAllColumns()) {
          type = rsMetaData.getType(i);
        }
        else {
          type = rsMetaData.getType(columns.get(i));
        }
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
      switch (type.getName()) {
        case UUID:
          final UUID id = row.getUUID(i);
          ((Setter<Object, UUID>)setters.get(i)).set(obj, id);
          break;
        case ASCII:
        case VARCHAR:
        case TEXT:
          final String ascii = row.getString(i);
          ((Setter<Object, String>)setters.get(i)).set(obj, ascii);
          break;
        case BOOLEAN:
          final boolean bool = row.getBool(i);
          ((SetterBoolean)setters.get(i)).set(obj, bool);
          break;
        case INT:
          final int intValue = row.getInt(i);
          ((SetterInt)setters.get(i)).set(obj, intValue);
          break;

        case BIGINT:
        case COUNTER:
          final long longValue = row.getLong(i);
          ((SetterLong)setters.get(i)).set(obj, longValue);
          break;
        case FLOAT:
          final float floatValue = row.getFloat(i);
          ((SetterFloat)setters.get(i)).set(obj, floatValue);
          break;
        case DOUBLE:
          final double doubleValue = row.getDouble(i);
          ((SetterDouble)setters.get(i)).set(obj, doubleValue);
          break;
        case DECIMAL:
          final BigDecimal decimal = row.getDecimal(i);
          ((Setter<Object, BigDecimal>)setters.get(i)).set(obj, decimal);
          break;
        case SET:
          Set<?> set = row.getSet(i, Object.class);
          ((Setter<Object, Set<?>>)setters.get(i)).set(obj, set);
          break;
        case MAP:
          final Map<?, ?> map = row.getMap(i, Object.class, Object.class);
          ((Setter<Object, Map<?, ?>>)setters.get(i)).set(obj, map);
          break;
        case LIST:
          final List<?> list = row.getList(i, Object.class);
          ((Setter<Object, List<?>>)setters.get(i)).set(obj, list);
          break;
        case TIMESTAMP:
          final Date date = row.getDate(i);
          ((Setter<Object, Date>)setters.get(i)).set(obj, date);
          break;
        default:
          throw new RuntimeException("unsupported data type " + type.getName());
      }
    }
    return obj;
  }

  @Override
  public String queryToRetrieveData()
  {
    boolean flag = false;

    switch (primaryKeyColumnType.getName()) {
      case INT:
        if (startRow.intValue() > lastRowIdInBatch.intValue()) {
          flag = true;
        }
        break;
      case COUNTER:
        if (startRow.longValue() > lastRowIdInBatch.longValue()) {
          flag = true;
        }
        break;
      case FLOAT:
        if (startRow.floatValue() > lastRowIdInBatch.floatValue()) {
          flag = true;
        }
        break;
      case DOUBLE:
        if (startRow.doubleValue() > lastRowIdInBatch.doubleValue()) {
          flag = true;
        }
        break;
    }

    if (flag) {
      return "";
    }

    startRow = lastRowIdInBatch.intValue() + 1;
    StringBuilder sb = new StringBuilder();
    sb.append(retrieveQuery).append(" where ").append("token(").append(primaryKeyColumn).append(")").append(">=").append(startRow).append(" LIMIT ").append(limit);
    logger.debug("retrievequery is {}", sb.toString());

    return sb.toString();
  }

  /*
   * Overriding processResult to save primarykey column value from last row in batch.
   */
  @Override
  protected void processResult(ResultSet result)
  {
      Row lastRowInBatch = null;
        for (Row row: result) {
          Object tuple = getTuple(row);
          outputPort.emit(tuple);
          lastRowInBatch = row;
        }
           if (lastRowInBatch != null) {
          switch (primaryKeyColumnType.getName()) {
            case INT:
              lastRowIdInBatch = lastRowInBatch.getInt(0);
              break;
            case COUNTER:
              lastRowIdInBatch = lastRowInBatch.getLong(0);
              break;
            case FLOAT:
              lastRowIdInBatch = lastRowInBatch.getFloat(0);
              break;
            case DOUBLE:
              lastRowIdInBatch = lastRowInBatch.getDouble(0);
              break;
            default:
              throw new RuntimeException("unsupported data type " + primaryKeyColumnType.getName());
          }
        }
  }

  private static final Logger logger = LoggerFactory.getLogger(CassandraPOJOInputOperator.class);
}
