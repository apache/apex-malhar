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
package com.datatorrent.lib.db.jdbc;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

import com.datatorrent.lib.util.FieldInfo;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;
import com.datatorrent.lib.util.PojoUtils.GetterBoolean;
import com.datatorrent.lib.util.PojoUtils.GetterDouble;
import com.datatorrent.lib.util.PojoUtils.GetterFloat;
import com.datatorrent.lib.util.PojoUtils.GetterInt;
import com.datatorrent.lib.util.PojoUtils.GetterLong;
import com.datatorrent.lib.util.PojoUtils.GetterShort;

import java.math.BigDecimal;
import java.sql.*;
import java.util.List;

import javax.validation.constraints.NotNull;

import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * <p>
 * JdbcPOJOOutputOperator class.</p>
 * A Generic implementation of AbstractJdbcTransactionableOutputOperator which takes in any POJO.
 *
 * @displayName Jdbc Output Operator
 * @category Output
 * @tags database, sql, pojo, jdbc
 * @since 2.1.0
 */
@Evolving
public class JdbcPOJOOutputOperator extends AbstractJdbcTransactionableOutputOperator<Object> implements Operator.ActivationListener<OperatorContext>
{
  @NotNull
  private List<FieldInfo> fieldInfos;

  private List<Integer> columnDataTypes;

  @NotNull
  private String tablename;

  private final transient List<JdbcPOJOInputOperator.ActiveFieldInfo> columnFieldGetters;

  private String insertStatement;

  private transient Class<?> pojoClass;

  @InputPortFieldAnnotation(optional = true, schemaRequired = true)
  public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
  {
    @Override
    public void setup(Context.PortContext context)
    {
      pojoClass = context.getValue(Context.PortContext.TUPLE_CLASS);
    }

    @Override
    public void process(Object t)
    {
      JdbcPOJOOutputOperator.super.input.process(t);
    }

  };

  @Override
  public void setup(OperatorContext context)
  {
    StringBuilder columns = new StringBuilder();
    StringBuilder values = new StringBuilder();
    for (int i = 0; i < fieldInfos.size(); i++) {
      columns.append(fieldInfos.get(i).getColumnName());
      values.append("?");
      if (i < fieldInfos.size() - 1) {
        columns.append(",");
        values.append(",");
      }
    }
    insertStatement = "INSERT INTO "
            + tablename
            + " (" + columns.toString() + ")"
            + " VALUES (" + values.toString() + ")";
    LOG.debug("insert statement is {}", insertStatement);

    super.setup(context);

    if (columnDataTypes == null) {
      try {
        populateColumnDataTypes(columns.toString());
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    for (FieldInfo fi : fieldInfos) {
      columnFieldGetters.add(new JdbcPOJOInputOperator.ActiveFieldInfo(fi));
    }
  }

  protected void populateColumnDataTypes(String columns) throws SQLException
  {
    columnDataTypes = Lists.newArrayList();
    try (Statement st = store.getConnection().createStatement()) {
      ResultSet rs = st.executeQuery("select " + columns + " from " + tablename);

      ResultSetMetaData rsMetaData = rs.getMetaData();
      LOG.debug("resultSet MetaData column count {}", rsMetaData.getColumnCount());

      for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
        int type = rsMetaData.getColumnType(i);
        columnDataTypes.add(type);
        LOG.debug("column name {} type {}", rsMetaData.getColumnName(i), type);
      }
    }
  }

  public JdbcPOJOOutputOperator()
  {
    super();
    columnFieldGetters = Lists.newArrayList();
  }

  @Override
  protected String getUpdateCommand()
  {
    LOG.debug("insert statement is {}", insertStatement);
    return insertStatement;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected void setStatementParameters(PreparedStatement statement, Object tuple) throws SQLException
  {
    final int size = columnDataTypes.size();
    for (int i = 0; i < size; i++) {
      final int type = columnDataTypes.get(i);
      JdbcPOJOInputOperator.ActiveFieldInfo activeFieldInfo = columnFieldGetters.get(i);
      switch (type) {
        case (Types.CHAR):
        case (Types.VARCHAR):
          statement.setString(i + 1, ((Getter<Object, String>) activeFieldInfo.setterOrGetter).get(tuple));
          break;

        case (Types.BOOLEAN):
          statement.setBoolean(i + 1, ((GetterBoolean<Object>) activeFieldInfo.setterOrGetter).get(tuple));
          break;

        case (Types.TINYINT):
          statement.setByte(i + 1, ((PojoUtils.GetterByte<Object>) activeFieldInfo.setterOrGetter).get(tuple));
          break;

        case (Types.SMALLINT):
          statement.setShort(i + 1, ((GetterShort<Object>) activeFieldInfo.setterOrGetter).get(tuple));
          break;

        case (Types.INTEGER):
          statement.setInt(i + 1, ((GetterInt<Object>) activeFieldInfo.setterOrGetter).get(tuple));
          break;

        case (Types.BIGINT):
          statement.setLong(i + 1, ((GetterLong<Object>) activeFieldInfo.setterOrGetter).get(tuple));
          break;

        case (Types.FLOAT):
          statement.setFloat(i + 1, ((GetterFloat<Object>) activeFieldInfo.setterOrGetter).get(tuple));
          break;

        case (Types.DOUBLE):
          statement.setDouble(i + 1, ((GetterDouble<Object>) activeFieldInfo.setterOrGetter).get(tuple));
          break;

        case Types.DECIMAL:
          statement.setBigDecimal(i + 1, ((Getter<Object, BigDecimal>) activeFieldInfo.setterOrGetter).get(tuple));
          break;

        case Types.TIMESTAMP:
          statement.setTimestamp(i + 1, new Timestamp(((GetterLong<Object>) activeFieldInfo.setterOrGetter).get(tuple)));
          break;

        case Types.TIME:
          statement.setTime(i + 1, new Time(((GetterLong<Object>) activeFieldInfo.setterOrGetter).get(tuple)));
          break;

        case Types.DATE:
          statement.setDate(i + 1, new Date(((GetterLong<Object>) activeFieldInfo.setterOrGetter).get(tuple)));
          break;

        default:
          handleUnknownDataType(type, tuple, activeFieldInfo);
          break;
      }
    }
  }

  @SuppressWarnings("UnusedParameters")
  protected void handleUnknownDataType(int type, Object tuple, JdbcPOJOInputOperator.ActiveFieldInfo activeFieldInfo)
  {
    throw new RuntimeException("unsupported data type " + type);
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
   * @useSchema $[].pojoFieldExpression input.fields[].name
   */
  public void setFieldInfos(List<FieldInfo> fieldInfos)
  {
    this.fieldInfos = fieldInfos;
  }

  /*
   * Gets the name of the table in database.
   */
  public String getTablename()
  {
    return tablename;
  }

  public void setTablename(String tablename)
  {
    this.tablename = tablename;
  }

  private static final Logger LOG = LoggerFactory.getLogger(JdbcPOJOOutputOperator.class);

  @Override
  public void activate(OperatorContext context)
  {
    final int size = columnDataTypes.size();
    for (int i = 0; i < size; i++) {
      final int type = columnDataTypes.get(i);
      JdbcPOJOInputOperator.ActiveFieldInfo activeFieldInfo = columnFieldGetters.get(i);
      switch (type) {
        case (Types.CHAR):
        case (Types.VARCHAR):
          activeFieldInfo.setterOrGetter = PojoUtils.createGetter(pojoClass, activeFieldInfo.fieldInfo.getPojoFieldExpression(),
            String.class);
          break;

        case (Types.BOOLEAN):
          activeFieldInfo.setterOrGetter = PojoUtils.createGetterBoolean(pojoClass, activeFieldInfo.fieldInfo.getPojoFieldExpression());
          break;

        case (Types.TINYINT):
          activeFieldInfo.setterOrGetter = PojoUtils.createGetterByte(pojoClass, activeFieldInfo.fieldInfo.getPojoFieldExpression());
          break;

        case (Types.SMALLINT):
          activeFieldInfo.setterOrGetter = PojoUtils.createGetterShort(pojoClass, activeFieldInfo.fieldInfo.getPojoFieldExpression());
          break;

        case (Types.INTEGER):
          activeFieldInfo.setterOrGetter = PojoUtils.createGetterInt(pojoClass, activeFieldInfo.fieldInfo.getPojoFieldExpression());
          break;

        case (Types.BIGINT):
          activeFieldInfo.setterOrGetter = PojoUtils.createGetterLong(pojoClass, activeFieldInfo.fieldInfo.getPojoFieldExpression());
          break;

        case (Types.FLOAT):
          activeFieldInfo.setterOrGetter = PojoUtils.createGetterFloat(pojoClass, activeFieldInfo.fieldInfo.getPojoFieldExpression());
          break;

        case (Types.DOUBLE):
          activeFieldInfo.setterOrGetter = PojoUtils.createGetterDouble(pojoClass, activeFieldInfo.fieldInfo.getPojoFieldExpression());
          break;

        case Types.DECIMAL:
          activeFieldInfo.setterOrGetter = PojoUtils.createGetter(pojoClass, activeFieldInfo.fieldInfo.getPojoFieldExpression(),
            BigDecimal.class);
          break;

        case Types.TIMESTAMP:
          activeFieldInfo.setterOrGetter = PojoUtils.createGetterLong(pojoClass, activeFieldInfo.fieldInfo.getPojoFieldExpression());
          break;

        case Types.TIME:
          activeFieldInfo.setterOrGetter = PojoUtils.createGetterLong(pojoClass, activeFieldInfo.fieldInfo.getPojoFieldExpression());
          break;

        case Types.DATE:
          activeFieldInfo.setterOrGetter = PojoUtils.createGetterLong(pojoClass, activeFieldInfo.fieldInfo.getPojoFieldExpression());
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
}
