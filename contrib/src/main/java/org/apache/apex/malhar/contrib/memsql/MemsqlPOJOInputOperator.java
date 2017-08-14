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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.apex.malhar.lib.util.PojoUtils;
import org.apache.apex.malhar.lib.util.PojoUtils.Setter;
import org.apache.apex.malhar.lib.util.PojoUtils.SetterBoolean;
import org.apache.apex.malhar.lib.util.PojoUtils.SetterDouble;
import org.apache.apex.malhar.lib.util.PojoUtils.SetterFloat;
import org.apache.apex.malhar.lib.util.PojoUtils.SetterInt;
import org.apache.apex.malhar.lib.util.PojoUtils.SetterLong;
import org.apache.apex.malhar.lib.util.PojoUtils.SetterShort;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import com.datatorrent.api.Context.OperatorContext;

/**
 * <p>
 * MemsqlPOJOInputOperator</p>
 *
 * A Generic implementation of AbstractMemsqlInputOperator which gets field values from memsql database columns and sets in a POJO.
 * User should also provide a query to fetch the rows from database. This query is run continuously to fetch new data and
 * hence should be parameterized. The parameters that can be used are %t for table name, %p for primary key, %s for start value
 * and %l for batchSize. The start value is continuously updated with the value of a primary key column of the last row from
 * the result of the previous run of the query. The primary key column is also identified by the user using a property.
 *
 * @displayName Memsql Input Operator
 * @category Input
 * @tags database, sql, pojo, memsql
 * @since 3.0.0
 */
@Evolving
public class MemsqlPOJOInputOperator extends AbstractMemsqlInputOperator<Object>
{
  @Min(1)
  private int batchSize = 10;
  @Min(0)
  private Number startRow = 0;
  @NotNull
  private List<String> expressions;
  @NotNull
  private String tablename;
  @NotNull
  private String primaryKeyColumn;
  @NotNull
  private List<String> columns;
  private transient Number lastRowKey;
  @NotNull
  private String query;
  /*
   * Mapping of Jdbc Data Types to Java Data Type. Example: {"BIGNT":java.lang.long}
   * It will remain same for all instances of this operator.
   */
  private static final Map<String, Class<?>> jdbcToJavaType = new HashMap<String, Class<?>>();

  // Mapping of Column Names to java class mapping. Example: {"name":String.class,"id":int.class}
  private final transient Map<String, Class<?>> columnNameToClassMapping;
  private final transient List<Object> setters;
  private transient Class<?> objectClass = null;
  private transient Class<?> primaryKeyColumnType;

  public List<String> getColumns()
  {
    return columns;
  }

  /**
   * The columns specified by user in case POJO needs to contain fields specific to these columns only.
   * User should specify columns in same order as expressions for fields.
   * @param columns The columns.
   */
  public void setColumns(List<String> columns)
  {
    this.columns = columns;
  }

  /**
   * Gets the primary key column of the input table.
   * @return The primary key column of the input table.
   */
  public String getPrimaryKeyColumn()
  {
    return primaryKeyColumn;
  }

  /**
   * The primary key column of the input table.
   * @param primaryKeyColumn The primary key column of the input table.
   */
  public void setPrimaryKeyColumn(String primaryKeyColumn)
  {
    this.primaryKeyColumn = primaryKeyColumn;
  }

  /**
   * The row to start reading from the input table at.
   * @return The row to start reading from the input table at.
   */
  public Number getStartRow()
  {
    return startRow;
  }

  /**
   * Sets the row to start reading form the input table at.
   * @param startRow The row to start reading from the input table at.
   */
  public void setStartRow(Number startRow)
  {
    this.startRow = startRow;
  }

  /**
   * Sets the batch size.
   * @param batchSize The batch size.
   */
  public void setBatchSize(int batchSize)
  {
    this.batchSize = batchSize;
  }

  /**
   * Records are read in batches of this size.
   * @return batchsize
   */
  public int getBatchSize()
  {
    return batchSize;
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

  /**
   * Gets the query used to extract data from memsql.
   * @return The query.
   */
  public String getQuery()
  {
    return query;
  }

  /**
   * Parameterized query with parameters such as %t for table name , %p for primary key, %s for start value and %l for batchSize.
   * Example of retrieveQuery:
   * select * from %t where %p > %s batchSize %l;
   */
  public void setQuery(String query)
  {
    this.query = query.replace("%t", tablename);
  }

  /**
   * Gets the getter expressions for extracting data from POJOs.
   * @return The getter expressions for extracting data from pojos.
   */
  public List<String> getExpressions()
  {
    return expressions;
  }

  /**
   * An ArrayList of Java expressions that will yield the memsql column value to be set in output object.
   * Each expression corresponds to one column in the Memsql table.
   */
  public void setExpressions(List<String> expressions)
  {
    this.expressions = expressions;
  }

  /**
   * Gets the name of the table that is read from memsql.
   * @return The name of the table that is read from memsql.
   */
  public String getTablename()
  {
    return tablename;
  }

  /**
   * The table name in memsql to read data from.
   * @param tablename The table name.
   */
  public void setTablename(String tablename)
  {
    this.tablename = tablename;
  }

  static {
    jdbcToJavaType.put("VARCHAR", String.class);
    jdbcToJavaType.put("CHAR", String.class);
    jdbcToJavaType.put("LONGTEXT", String.class);
    jdbcToJavaType.put("INT", int.class);
    jdbcToJavaType.put("BIGINT", Long.class);
    jdbcToJavaType.put("DATE", Date.class);
    jdbcToJavaType.put("TIME", Time.class);
    jdbcToJavaType.put("TIMESTAMP", Timestamp.class);
    jdbcToJavaType.put("NUMERIC", BigDecimal.class);
    jdbcToJavaType.put("DECIMAL", BigDecimal.class);
    jdbcToJavaType.put("BOOL", Boolean.class);
    jdbcToJavaType.put("TINYINT", Byte.class);
    jdbcToJavaType.put("BIT", Boolean.class);
    jdbcToJavaType.put("SMALLINT", Short.class);
    jdbcToJavaType.put("MEDIUMINT", Short.class);
    jdbcToJavaType.put("DOUBLE", Double.class);
    jdbcToJavaType.put("FLOAT", Float.class);
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    try {
      Statement statement = store.getConnection().createStatement();
      ResultSet resultSet = statement.executeQuery("describe " + tablename);
      while (resultSet.next()) {
        String memsqlType = resultSet.getString("Type");
        String javaType;
        if (memsqlType.contains("(")) {
          javaType = memsqlType.substring(0, memsqlType.indexOf('(')).toUpperCase();
        } else {
          javaType = memsqlType.toUpperCase();
        }

        Class<?> type = jdbcToJavaType.get(javaType);
        String columnNameInTable = resultSet.getString("Field");
        columnNameToClassMapping.put(columnNameInTable, type);
        if (resultSet.getString("Key").equals("PRI")) {
          primaryKeyColumnType = type;
        }
      }
      if (primaryKeyColumnType == null) {
        throw new RuntimeException("Primary Key is not defined on the specified table");
      }

      if (query.contains("%p")) {
        query = query.replace("%p", primaryKeyColumn);
      }
      if (query.contains("%l")) {
        query = query.replace("%l", batchSize + "");
      }

      statement.close();
    } catch (SQLException ex) {
      throw new RuntimeException(ex);
    }

    try {
      // This code will be replaced after integration of creating POJOs on the fly utility.
      objectClass = Class.forName(outputClass);
    } catch (ClassNotFoundException ex) {
      throw new RuntimeException(ex);
    }

    //In case columns is a subset
    for (int i = 0; i < columns.size(); i++) {
      final String setterExpression = expressions.get(i);
      String columnName = columns.get(i);
      setters.add(PojoUtils.constructSetter(objectClass, setterExpression, columnNameToClassMapping.get(columnName)));

    }

  }

  public MemsqlPOJOInputOperator()
  {
    super();
    setters = new ArrayList<Object>();
    columnNameToClassMapping = new HashMap<String, Class<?>>();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object getTuple(ResultSet result)
  {
    Object obj;
    try {
      // This code will be replaced after integration of creating POJOs on the fly utility.
      obj = objectClass.newInstance();
    } catch (InstantiationException ex) {
      throw new RuntimeException(ex);
    } catch (IllegalAccessException ex) {
      throw new RuntimeException(ex);
    }
    final int size = columns.size();

    try {
      for (int i = 0; i < size; i++) {
        String columnName = columns.get(i);
        Class<?> classType = columnNameToClassMapping.get(columnName);
        if (classType == String.class) {
          ((Setter<Object, String>)setters.get(i)).set(obj, result.getString(columnName));
        } else if (classType == int.class) {
          ((SetterInt)setters.get(i)).set(obj, result.getInt(columnName));
        } else if (classType == Boolean.class) {
          ((SetterBoolean)setters.get(i)).set(obj, result.getBoolean(columnName));
        } else if (classType == Short.class) {
          ((SetterShort)setters.get(i)).set(obj, result.getShort(columnName));
        } else if (classType == Long.class) {
          ((SetterLong)setters.get(i)).set(obj, result.getLong(columnName));
        } else if (classType == Float.class) {
          ((SetterFloat)setters.get(i)).set(obj, result.getFloat(columnName));
        } else if (classType == Double.class) {
          ((SetterDouble)setters.get(i)).set(obj, result.getDouble(columnName));
        } else if (classType == BigDecimal.class) {
          ((Setter<Object, BigDecimal>)setters.get(i)).set(obj, result.getBigDecimal(columnName));
        } else if (classType == Date.class) {
          ((Setter<Object, Date>)setters.get(i)).set(obj, result.getDate(columnName));
        } else if (classType == Timestamp.class) {
          ((Setter<Object, Timestamp>)setters.get(i)).set(obj, result.getTimestamp(columnName));
        } else {
          throw new RuntimeException("unsupported data type ");
        }
      }

      if (result.isLast()) {
        logger.debug("last row is {}", lastRowKey);
        if (primaryKeyColumnType == int.class) {
          lastRowKey = result.getInt(primaryKeyColumn);
        } else if (primaryKeyColumnType == Long.class) {
          lastRowKey = result.getLong(primaryKeyColumn);
        } else if (primaryKeyColumnType == Float.class) {
          lastRowKey = result.getFloat(primaryKeyColumn);
        } else if (primaryKeyColumnType == Double.class) {
          lastRowKey = result.getDouble(primaryKeyColumn);
        } else if (primaryKeyColumnType == Short.class) {
          lastRowKey = result.getShort(primaryKeyColumn);
        } else {
          throw new RuntimeException("unsupported data type ");
        }
      }
    } catch (SQLException ex) {
      throw new RuntimeException(ex);
    }
    return obj;
  }

  /**
   * This method replaces the parameters in Query with actual values given by user.
   * Example of retrieveQuery:
   * select * from %t where %p > %s batchSize %l;
   */
  @Override
  public String queryToRetrieveData()
  {
    String parameterizedQuery;
    if (query.contains("%s")) {
      parameterizedQuery = query.replace("%s", startRow + "");
    } else {
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
