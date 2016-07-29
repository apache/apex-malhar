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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceStability.Evolving;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.db.jdbc.JdbcPOJOInputOperator.ActiveFieldInfo;
import com.datatorrent.lib.util.FieldInfo;
import com.datatorrent.lib.util.PojoUtils;

/**
 * A concrete implementation for {@link AbstractJdbcPollInputOperator} for
 * consuming data from MySQL using JDBC interface <br>
 * User needs to provide tableName,dbConnection,setEmitColumnList,look-up key <br>
 * Optionally batchSize,pollInterval,Look-up key and a where clause can be given <br>
 * This operator uses static partitioning to arrive at range queries for exactly
 * once reads<br>
 * Assumption is that there is an ordered column using which range queries can
 * be formed<br>
 * If an emitColumnList is provided, please ensure that the keyColumn is the
 * first column in the list<br>
 * Range queries are formed using the {@link JdbcMetaDataUtility} Output - comma
 * separated list of the emit columns eg columnA,columnB,columnC
 * 
 * @displayName Jdbc Polling Input Operator
 * @category Input
 * @tags database, sql, jdbc
 */
@Evolving
@OperatorAnnotation(checkpointableWithinAppWindow = false)
public class JdbcPOJOPollInputOperator extends AbstractJdbcPollInputOperator<Object>
{
  private final transient List<ActiveFieldInfo> columnFieldSetters = Lists.newArrayList();
  protected List<Integer> columnDataTypes;
  protected transient Class<?> pojoClass;
  @NotNull
  private List<FieldInfo> fieldInfos;

  @OutputPortFieldAnnotation(schemaRequired = true)
  public final transient DefaultOutputPort<Object> outputPort = new DefaultOutputPort<Object>()
  {
    @Override
    public void setup(Context.PortContext context)
    {
      pojoClass = context.getValue(Context.PortContext.TUPLE_CLASS);
    }
  };

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    try {
      // closing the query statement in super class as it is not needed
      if (getColumnsExpression() == null) {
        StringBuilder columns = new StringBuilder();
        for (int i = 0; i < fieldInfos.size(); i++) {
          columns.append(fieldInfos.get(i).getColumnName());
          if (i < fieldInfos.size() - 1) {
            columns.append(",");
          }
        }
        setColumnsExpression(columns.toString());
        LOG.debug("select expr {}", columns.toString());
      }

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
              activeFieldInfo.fieldInfo.getPojoFieldExpression(), String.class);
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
              activeFieldInfo.fieldInfo.getPojoFieldExpression(), BigDecimal.class);
          break;

        case Types.TIMESTAMP:
          activeFieldInfo.setterOrGetter = PojoUtils.createSetter(pojoClass,
              activeFieldInfo.fieldInfo.getPojoFieldExpression(), Timestamp.class);
          break;

        case Types.TIME:
          activeFieldInfo.setterOrGetter = PojoUtils.createSetter(pojoClass,
              activeFieldInfo.fieldInfo.getPojoFieldExpression(), Time.class);
          break;

        case Types.DATE:
          activeFieldInfo.setterOrGetter = PojoUtils.createSetter(pojoClass,
              activeFieldInfo.fieldInfo.getPojoFieldExpression(), Date.class);
          break;

        default:
          throw new RuntimeException("unsupported data type " + type);
      }
    }
    super.activate(context);
  }

  protected void populateColumnDataTypes() throws SQLException
  {
    columnDataTypes = Lists.newArrayList();
    PreparedStatement stmt = store.getConnection().prepareStatement(buildRangeQuery(getKey(), 1, 1));
    try (ResultSet rs = stmt.executeQuery()) {
      Map<String, Integer> nameToType = Maps.newHashMap();
      ResultSetMetaData rsMetaData = rs.getMetaData();
      LOG.debug("resultSet MetaData column count {}", rsMetaData.getColumnCount());

      for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
        int type = rsMetaData.getColumnType(i);
        String name = rsMetaData.getColumnName(i);
        LOG.debug("column name {} type {}", name, type);
        nameToType.put(name, type);
      }

      for (FieldInfo fieldInfo : fieldInfos) {
        columnDataTypes.add(nameToType.get(fieldInfo.getColumnName()));
      }
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
            throw new RuntimeException("unsupported data type " + type);
        }
      }
      return obj;
    } catch (SQLException e) {
      store.disconnect();
      throw new RuntimeException("fetching metadata", e);
    }
  }

  @Override
  protected void emitTuple(Object obj)
  {
    outputPort.emit(obj);
  }

  public List<FieldInfo> getFieldInfos()
  {
    return fieldInfos;
  }

  public void setFieldInfos(List<FieldInfo> fieldInfos)
  {
    this.fieldInfos = fieldInfos;
  }

  private static final Logger LOG = LoggerFactory.getLogger(JdbcPOJOPollInputOperator.class);
}
