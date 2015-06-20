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
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * MemsqlPOJOInputOperator</p>
 * A Generic implementation of AbstractMemsqlInputOperator which gets field values from memsql database columns and sets in a POJO.
 *
 * @displayName Memsql POJO Input Operator
 * @category Input
 * @tags input operator
 */
public class MemsqlPOJOInputOperator extends AbstractMemsqlInputOperator<Object>
{
  private final transient ArrayList<Integer> columnDataTypes;
  @Min(1)
  private int limit = 10;
  @Min(1)
  private int startRow = 1;
  @NotNull
  private ArrayList<String> expressions;
  @NotNull
  private String tablename;
  private transient ArrayList<Object> setters;
  @NotNull
  private String retrieveQuery;
  private transient Class<?> objectClass = null;
  @NotNull
  private String primaryKeyColumn;
  private ArrayList<String> columns;
  private boolean useAllColumns;

  /*
   * Subset of columns specified by User in case POJO needs to contain fields specific to these columns only.
   */
  public ArrayList<String> getColumns()
  {
    return columns;
  }

  public void setColumns(ArrayList<String> columns)
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
  public int getStartRow()
  {
    return startRow;
  }

  public void setStartRow(int startRow)
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
  public ArrayList<String> getExpressions()
  {
    return expressions;
  }

  public void setExpressions(ArrayList<String> expressions)
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
      resultSet = statement.executeQuery("select count(*) from " + tablename);
      resultSet.next();
      resultSet = statement.executeQuery("describe " + tablename);
      rsMetaData = resultSet.getMetaData();

      if (!isUseAllColumns()) {
        numberOfColumns = rsMetaData.getColumnCount();
      }
      else {
        numberOfColumns = columns.size();
      }

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
        if (!isUseAllColumns()) {
          type = rsMetaData.getColumnType(i);
        }
        else {
          type = rsMetaData.getColumnType(resultSet.findColumn(columns.get(i)));
        }
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
      final int type = columnDataTypes.get(i);
      try {
        switch (type) {
          case (Types.CHAR):
          case (Types.VARCHAR):
            final String ascii;

            ascii = result.getString(i + 1);

            ((Setter<Object, String>)setters.get(i)).set(obj, ascii);
            break;
          case (Types.BOOLEAN):
          case (Types.TINYINT):
            final boolean boolValue;
            boolValue = result.getBoolean(i);

            ((SetterBoolean)setters.get(i)).set(obj, boolValue);
            break;
          case (Types.SMALLINT):
            final short shortValue;

            shortValue = result.getShort(i);

            ((SetterShort)setters.get(i)).set(obj, shortValue);
            break;
          case (Types.INTEGER):
            final int intValue;

            intValue = result.getInt(i + 1);

            ((SetterInt)setters.get(i)).set(obj, intValue);
            break;
          case (Types.BIGINT):
            final long longValue;
            longValue = result.getLong(i);

            ((SetterLong)setters.get(i)).set(obj, longValue);
            break;
          case (Types.FLOAT):
            final float floatValue;

            floatValue = result.getFloat(i);
            ((SetterFloat)setters.get(i)).set(obj, floatValue);
            break;
          case (Types.DOUBLE):
            final double doubleValue;
            doubleValue = result.getDouble(i);

            ((SetterDouble)setters.get(i)).set(obj, doubleValue);
            break;
          case (Types.DECIMAL):
            final BigDecimal decimal;
            decimal = result.getBigDecimal(i);

            ((Setter<Object, BigDecimal>)setters.get(i)).set(obj, decimal);
            break;
          case (Types.DATE):
            final Date dateValue;
            dateValue = result.getDate(i);

            ((Setter<Object, Date>)setters.get(i)).set(obj, dateValue);
            break;
          case (Types.TIMESTAMP):
            final Timestamp timeValue;
            timeValue = result.getTimestamp(i);

            ((Setter<Object, Timestamp>)setters.get(i)).set(obj, timeValue);
            break;
          case (Types.OTHER):
            final Object objValue;
            objValue = result.getObject(i);

            ((Setter<Object, Object>)setters.get(i)).set(obj, objValue);
            break;
          default:
            throw new RuntimeException("unsupported data type " + type);
        }
      }
      catch (SQLException ex) {
        throw new RuntimeException(ex);
      }
    }
    return obj;
  }

  @Override
  public String queryToRetrieveData()
  {
    try {
      ResultSet rset = store.getConnection().createStatement().executeQuery("SELECT * FROM " + tablename + " ORDER BY " + primaryKeyColumn + " DESC LIMIT 1");
      if (startRow > rset.getInt(0)) {
        return null;
      }
    }
    catch (SQLException ex) {
      throw new RuntimeException(ex);
    }

    int endRow = startRow + limit;

    StringBuilder sb = new StringBuilder();
    sb.append(retrieveQuery);
    sb.append(" where ");
    sb.append(primaryKeyColumn);
    sb.append(" >= ");
    sb.append(startRow);
    sb.append(" and ");
    sb.append(primaryKeyColumn);
    sb.append(" < ");
    sb.append(endRow);
    startRow = endRow;

    return sb.toString();
  }

  private static final Logger logger = LoggerFactory.getLogger(MemsqlPOJOInputOperator.class);

}
