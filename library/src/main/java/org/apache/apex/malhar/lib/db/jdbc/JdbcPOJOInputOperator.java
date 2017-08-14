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
package org.apache.apex.malhar.lib.db.jdbc;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.util.FieldInfo;
import org.apache.apex.malhar.lib.util.PojoUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * A concrete input operator that reads from a database through the JDBC API.<br/>
 * This operator by default uses the {@link #fieldInfos} and the table name to construct the sql query.<br/>
 *
 * A user can provide there own query by setting the {@link #query} property which takes precedence.<br/>
 *
 * For eg. user can set the query property to a complex one : "select x1, x2 from t1, t2 where t1.x3 = t2.x3 ;"<br/>
 *
 * This implementation is generic so it uses offset/limit mechanism for batching which is not optimal. Batching is
 * most efficient when the tables/views are indexed and the query uses this information to retrieve data.<br/>
 * This can be achieved in sub-classes by overriding {@link #queryToRetrieveData()} and {@link #setRuntimeParams()}.
 *
 * @displayName Jdbc Input Operator
 * @category Input
 * @tags database, sql, pojo, jdbc
 * @since 2.1.0
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class JdbcPOJOInputOperator extends AbstractJdbcInputOperator<Object>
    implements Operator.ActivationListener<Context.OperatorContext>
{
  private static int DEF_FETCH_SIZE = 100;

  //table and query definition
  private String tableName;
  private String whereCondition;
  private String groupByClause;
  private String havingCondition;
  private String orderByExpr;
  private String query;
  private boolean mysqlSyntax;

  @NotNull
  private List<FieldInfo> fieldInfos = new ArrayList<>();;

  @Min(1)
  private int fetchSize;
  private int fetchDirection;

  private final transient List<ActiveFieldInfo> columnFieldSetters;
  private transient boolean windowDone;

  protected String columnsExpression;
  protected List<Integer> columnDataTypes;

  private transient PreparedStatement preparedStatement;
  protected transient Class<?> pojoClass;

  @AutoMetric
  protected long tuplesRead;

  @OutputPortFieldAnnotation(schemaRequired = true)
  public final transient DefaultOutputPort<Object> outputPort = new DefaultOutputPort<Object>()
  {
    @Override
    public void setup(Context.PortContext context)
    {
      pojoClass = context.getValue(Context.PortContext.TUPLE_CLASS);
    }
  };

  public JdbcPOJOInputOperator()
  {
    super();
    fetchSize = DEF_FETCH_SIZE;
    columnFieldSetters = Lists.newArrayList();
    mysqlSyntax = true;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    Preconditions.checkArgument(query != null || tableName != null, "both query and table name are not set");
    super.setup(context);

    try {
      //closing the query statement in super class as it is not needed
      queryStatement.close();
      if (query == null && columnsExpression == null) {
        StringBuilder columns = new StringBuilder();
        for (int i = 0; i < fieldInfos.size(); i++) {
          columns.append(fieldInfos.get(i).getColumnName());
          if (i < fieldInfos.size() - 1) {
            columns.append(",");
          }
        }
        columnsExpression = columns.toString();
        LOG.debug("select expr {}", columnsExpression);
      }

      preparedStatement = store.connection.prepareStatement(queryToRetrieveData());
      if (columnDataTypes == null) {
        populateColumnDataTypes();
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    for (FieldInfo fi : fieldInfos) {
      columnFieldSetters.add(new ActiveFieldInfo(fi));
    }
  }

  protected void populateColumnDataTypes() throws SQLException
  {
    columnDataTypes = Lists.newArrayList();
    preparedStatement.setMaxRows(0);
    setRuntimeParams();
    try (ResultSet rs = preparedStatement.executeQuery()) {
      Map<String, Integer> nameToType = Maps.newHashMap();
      ResultSetMetaData rsMetaData = rs.getMetaData();
      LOG.debug("resultSet MetaData column count {}", rsMetaData.getColumnCount());

      for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
        int type = rsMetaData.getColumnType(i);
        String name = rsMetaData.getColumnName(i);
        LOG.debug("column name {} type {}", name, type);
        if (query == null) {
          columnDataTypes.add(type);
        } else {
          //when it is a custom query we need to ensure the types are in the same order as field infos
          nameToType.put(name, type);
        }
      }

      if (query != null) {
        for (FieldInfo fieldInfo : fieldInfos) {
          columnDataTypes.add(nameToType.get(fieldInfo.getColumnName()));
        }
      }
    }
    preparedStatement.setFetchSize(fetchSize);
    preparedStatement.setMaxRows(fetchSize);
  }

  @Override
  public void beginWindow(long l)
  {
    windowDone = false;
  }

  @Override
  public void emitTuples()
  {
    if (!windowDone) {
      try {
        setRuntimeParams();
        ResultSet resultSet = preparedStatement.executeQuery();
        if (resultSet.next()) {
          do {
            Object tuple = getTuple(resultSet);
            outputPort.emit(tuple);
            tuplesRead++;
          }
          while (resultSet.next());
        } else {
          windowDone = true;
        }
        resultSet.close();
      } catch (SQLException ex) {
        store.disconnect();
        throw new RuntimeException(ex);
      }
    }
  }

  protected void setRuntimeParams() throws SQLException
  {
    if (mysqlSyntax) {
      preparedStatement.setLong(1, tuplesRead);
    } else {
      preparedStatement.setLong(1, tuplesRead);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object getTuple(ResultSet result)
  {
    Object obj;
    try {
      obj = pojoClass.newInstance();
    } catch (InstantiationException | IllegalAccessException ex) {
      store.disconnect();
      throw new RuntimeException(ex);
    }

    try {
      for (int i = 0; i < fieldInfos.size(); i++) {
        int type = columnDataTypes.get(i);
        ActiveFieldInfo afi = columnFieldSetters.get(i);

        switch (type) {
          case Types.CHAR:
          case Types.VARCHAR:
            String strVal = result.getString(i + 1);
            ((PojoUtils.Setter<Object, String>)afi.setterOrGetter).set(obj, strVal);
            break;

          case Types.BOOLEAN:
            boolean boolVal = result.getBoolean(i + 1);
            ((PojoUtils.SetterBoolean<Object>)afi.setterOrGetter).set(obj, boolVal);
            break;

          case Types.TINYINT:
            byte byteVal = result.getByte(i + 1);
            ((PojoUtils.SetterByte<Object>)afi.setterOrGetter).set(obj, byteVal);
            break;

          case Types.SMALLINT:
            short shortVal = result.getShort(i + 1);
            ((PojoUtils.SetterShort<Object>)afi.setterOrGetter).set(obj, shortVal);
            break;

          case Types.INTEGER:
            int intVal = result.getInt(i + 1);
            ((PojoUtils.SetterInt<Object>)afi.setterOrGetter).set(obj, intVal);
            break;

          case Types.BIGINT:
            long longVal = result.getLong(i + 1);
            ((PojoUtils.SetterLong<Object>)afi.setterOrGetter).set(obj, longVal);
            break;

          case Types.FLOAT:
            float floatVal = result.getFloat(i + 1);
            ((PojoUtils.SetterFloat<Object>)afi.setterOrGetter).set(obj, floatVal);
            break;

          case Types.DOUBLE:
            double doubleVal = result.getDouble(i + 1);
            ((PojoUtils.SetterDouble<Object>)afi.setterOrGetter).set(obj, doubleVal);
            break;

          case Types.DECIMAL:
            BigDecimal bdVal = result.getBigDecimal(i + 1);
            ((PojoUtils.Setter<Object, BigDecimal>)afi.setterOrGetter).set(obj, bdVal);
            break;

          case Types.TIMESTAMP:
            Timestamp tsVal = result.getTimestamp(i + 1);
            ((PojoUtils.Setter<Object, Timestamp>)afi.setterOrGetter).set(obj, tsVal);
            break;

          case Types.TIME:
            Time timeVal = result.getTime(i + 1);
            ((PojoUtils.Setter<Object, Time>)afi.setterOrGetter).set(obj, timeVal);
            break;

          case Types.DATE:
            Date dateVal = result.getDate(i + 1);
            ((PojoUtils.Setter<Object, Date>)afi.setterOrGetter).set(obj, dateVal);
            break;

          default:
            handleUnknownDataType(type, obj, afi);
            break;
        }
      }
      return obj;
    } catch (SQLException e) {
      store.disconnect();
      throw new RuntimeException("fetching metadata", e);
    }
  }

  @SuppressWarnings("UnusedParameters")
  protected void handleUnknownDataType(int type, Object tuple, ActiveFieldInfo activeFieldInfo)
  {
    throw new RuntimeException("unsupported data type " + type);
  }

  @Override
  public String queryToRetrieveData()
  {
    StringBuilder builder = new StringBuilder();

    if (query != null) {
      builder.append(query.trim());
      if (builder.charAt(builder.length() - 1) == ';') {
        builder.deleteCharAt(builder.length() - 1);
      }
    } else {
      builder.append("SELECT ").append(columnsExpression).append(" FROM ").append(tableName);
      if (whereCondition != null) {
        builder.append(" WHERE ").append(whereCondition);
      }
      if (groupByClause != null) {
        builder.append(" GROUP BY ").append(groupByClause);
        if (havingCondition != null) {
          builder.append(" HAVING ").append(havingCondition);
        }
      }
      if (orderByExpr != null) {
        builder.append(" ORDER BY ").append(orderByExpr);
      }
    }
    if (mysqlSyntax) {
      builder.append(" LIMIT ").append(fetchSize).append(" OFFSET ?");
    } else {
      builder.append(" OFFSET ? ROWS FETCH NEXT ").append(fetchSize).append(" ROWS ONLY");
    }
    builder.append(";");
    String queryStr = builder.toString();
    LOG.debug("built query {}", queryStr);
    return queryStr;
  }

  @Override
  public void activate(Context.OperatorContext context)
  {
    for (int i = 0; i < columnDataTypes.size(); i++) {
      final int type = columnDataTypes.get(i);
      JdbcPOJOInputOperator.ActiveFieldInfo activeFieldInfo = columnFieldSetters.get(i);
      switch (type) {
        case (Types.CHAR):
        case (Types.VARCHAR):
          activeFieldInfo.setterOrGetter = PojoUtils.createSetter(pojoClass,
              activeFieldInfo.fieldInfo.getPojoFieldExpression(),
            String.class);
          break;

        case (Types.BOOLEAN):
          activeFieldInfo.setterOrGetter = PojoUtils.createSetterBoolean(pojoClass,
              activeFieldInfo.fieldInfo.getPojoFieldExpression());
          break;

        case (Types.TINYINT):
          activeFieldInfo.setterOrGetter = PojoUtils.createSetterByte(pojoClass,
              activeFieldInfo.fieldInfo.getPojoFieldExpression());
          break;

        case (Types.SMALLINT):
          activeFieldInfo.setterOrGetter = PojoUtils.createSetterShort(pojoClass,
              activeFieldInfo.fieldInfo.getPojoFieldExpression());
          break;

        case (Types.INTEGER):
          activeFieldInfo.setterOrGetter = PojoUtils.createSetterInt(pojoClass,
              activeFieldInfo.fieldInfo.getPojoFieldExpression());
          break;

        case (Types.BIGINT):
          activeFieldInfo.setterOrGetter = PojoUtils.createSetterLong(pojoClass,
              activeFieldInfo.fieldInfo.getPojoFieldExpression());
          break;

        case (Types.FLOAT):
          activeFieldInfo.setterOrGetter = PojoUtils.createSetterFloat(pojoClass,
              activeFieldInfo.fieldInfo.getPojoFieldExpression());
          break;

        case (Types.DOUBLE):
          activeFieldInfo.setterOrGetter = PojoUtils.createSetterDouble(pojoClass,
              activeFieldInfo.fieldInfo.getPojoFieldExpression());
          break;

        case Types.DECIMAL:
          activeFieldInfo.setterOrGetter = PojoUtils.createSetter(pojoClass,
              activeFieldInfo.fieldInfo.getPojoFieldExpression(),
              BigDecimal.class);
          break;

        case Types.TIMESTAMP:
          activeFieldInfo.setterOrGetter = PojoUtils.createSetter(pojoClass,
              activeFieldInfo.fieldInfo.getPojoFieldExpression(),Timestamp.class);
          break;

        case Types.TIME:
          activeFieldInfo.setterOrGetter = PojoUtils.createSetter(pojoClass,
              activeFieldInfo.fieldInfo.getPojoFieldExpression(),Time.class);
          break;

        case Types.DATE:
          activeFieldInfo.setterOrGetter = PojoUtils.createSetter(pojoClass,
              activeFieldInfo.fieldInfo.getPojoFieldExpression(), Date.class);
          break;

        default:
          handleUnknownDataType(type, null, activeFieldInfo);
          break;
      }
    }
  }

  @Override
  public void deactivate()
  {
  }

  protected static class ActiveFieldInfo
  {
    final FieldInfo fieldInfo;
    Object setterOrGetter;

    ActiveFieldInfo(FieldInfo fieldInfo)
    {
      this.fieldInfo = fieldInfo;
    }
  }

  /**
   * A list of {@link FieldInfo}s where each item maps a column name to a pojo field name.
   */
  public List<FieldInfo> getFieldInfos()
  {
    return fieldInfos;
  }

  /**
   * Sets the {@link FieldInfo}s. A {@link FieldInfo} maps a store column to a pojo field name.<br/>
   * The value from fieldInfo.column is assigned to fieldInfo.pojoFieldExpression.
   *
   * @description $[].columnName name of the database column name
   * @description $[].pojoFieldExpression pojo field name or expression
   * @useSchema $[].pojoFieldExpression outputPort.fields[].name
   */
  public void setFieldInfos(List<FieldInfo> fieldInfos)
  {
    this.fieldInfos = fieldInfos;
  }

  /**
   * @return table name
   */
  public String getTableName()
  {
    return tableName;
  }

  /**
   * Sets the table name.
   *
   * @param tableName table name
   */
  public void setTableName(String tableName)
  {
    this.tableName = tableName;
  }

  /**
   * @return where condition
   */
  public String getWhereCondition()
  {
    return whereCondition;
  }

  /**
   * Sets the where condition.
   *
   * @param whereCondition where condition.
   */
  public void setWhereCondition(String whereCondition)
  {
    this.whereCondition = whereCondition;
  }

  /**
   * @return group-by clause
   */
  public String getGroupByClause()
  {
    return groupByClause;
  }

  /**
   * Sets the group by clause.
   *
   * @param groupByClause group-by clause.
   */
  public void setGroupByClause(String groupByClause)
  {
    this.groupByClause = groupByClause;
  }

  /**
   * @return having condition
   */
  public String getHavingCondition()
  {
    return havingCondition;
  }

  /**
   * Sets the having condition.
   *
   * @param havingCondition having condition
   */
  public void setHavingCondition(String havingCondition)
  {
    this.havingCondition = havingCondition;
  }

  /**
   * @return order by expression.
   */
  public String getOrderByExpr()
  {
    return orderByExpr;
  }

  /**
   * Sets the order by expression.
   *
   * @param orderByExpr order by expression.
   */
  public void setOrderByExpr(String orderByExpr)
  {
    this.orderByExpr = orderByExpr;
  }

  /**
   * @return query
   */
  public String getQuery()
  {
    return query;
  }

  /**
   * Sets the query
   *
   * @param query query
   */
  public void setQuery(String query)
  {
    this.query = query;
  }

  /**
   * @return fetch size which is the number of rows retrieved from the database in a window.
   */
  public int getFetchSize()
  {
    return fetchSize;
  }

  /**
   * Sets the fetch size which is the number of rows retrieved from the database in a window.
   *
   * @param fetchSize number of rows retrieved from db in a window.
   */
  public void setFetchSize(int fetchSize)
  {
    this.fetchSize = fetchSize;
  }

  /**
   * @return fetch direction
   */
  public int getFetchDirection()
  {
    return fetchDirection;
  }

  /**
   * This sets the direction used in processing a result set. It allows the JDBC driver to optimize its processing.
   * Refer {@link PreparedStatement#setFetchDirection(int)}
   *
   * @param fetchDirection fetch direction
   */
  public void setFetchDirection(int fetchDirection)
  {
    this.fetchDirection = fetchDirection;
  }

  /**
   * @return is syntax mysql
   */
  public boolean isMysqlSyntax()
  {
    return mysqlSyntax;
  }

  /**
   * Sets the syntax of the query.
   *
   * @param mysqlSyntax true if it is mySql; when false oracle syntax is used.
   */
  public void setMysqlSyntax(boolean mysqlSyntax)
  {
    this.mysqlSyntax = mysqlSyntax;
  }

  /**
   * Function to initialize the list of {@link FieldInfo} externally from configuration/properties file.
   * Example entry in the properties/configuration file:
   <property>
   <name>dt.operator.JdbcPOJOInput.fieldInfosItem[0]</name>
   <value>
   {
   "columnName":"ID",
   "pojoFieldExpression": "id",
   "type":"INTEGER"
   }
   </value>
   </property>
   * @param index is the index in the list which is to be initialized.
   * @param value is the JSON String with appropriate mappings for {@link FieldInfo}.
   */
  public void setFieldInfosItem(int index, String value)
  {
    try {
      JSONObject jo = new JSONObject(value);
      FieldInfo fieldInfo = new FieldInfo(jo.getString("columnName"), jo.getString("pojoFieldExpression"),
          FieldInfo.SupportType.valueOf(jo.getString("type")));
      final int need = index - fieldInfos.size() + 1;
      for (int i = 0; i < need; i++) {
        fieldInfos.add(null);
      }
      fieldInfos.set(index,fieldInfo);
    } catch (Exception e) {
      throw new RuntimeException("Exception in setting FieldInfo " + value + " " + e.getMessage());
    }
  }

  public static final Logger LOG = LoggerFactory.getLogger(JdbcPOJOInputOperator.class);
}
