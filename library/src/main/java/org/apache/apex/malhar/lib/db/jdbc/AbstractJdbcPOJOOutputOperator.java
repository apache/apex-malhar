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
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import javax.validation.constraints.NotNull;

import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.util.FieldInfo;
import org.apache.apex.malhar.lib.util.PojoUtils;
import org.apache.apex.malhar.lib.util.PojoUtils.Getter;
import org.apache.apex.malhar.lib.util.PojoUtils.GetterBoolean;
import org.apache.apex.malhar.lib.util.PojoUtils.GetterDouble;
import org.apache.apex.malhar.lib.util.PojoUtils.GetterFloat;
import org.apache.apex.malhar.lib.util.PojoUtils.GetterInt;
import org.apache.apex.malhar.lib.util.PojoUtils.GetterLong;
import org.apache.apex.malhar.lib.util.PojoUtils.GetterShort;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

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
@org.apache.hadoop.classification.InterfaceStability.Evolving
public abstract class AbstractJdbcPOJOOutputOperator extends AbstractJdbcTransactionableOutputOperator<Object>
{
  private List<JdbcFieldInfo> fieldInfos = new ArrayList<>();
  protected List<Integer> columnDataTypes;

  @NotNull
  private String tablename;

  protected final transient List<ActiveFieldInfo> columnFieldGetters;

  protected transient Class<?> pojoClass;

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
      AbstractJdbcPOJOOutputOperator.super.input.process(t);
    }

  };

  public AbstractJdbcPOJOOutputOperator()
  {
    super();
    columnFieldGetters = Lists.newArrayList();
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

  @Override
  @SuppressWarnings("unchecked")
  protected void setStatementParameters(PreparedStatement statement, Object tuple) throws SQLException
  {
    final int size = columnDataTypes.size();
    for (int i = 0; i < size; i++) {
      final int type = columnDataTypes.get(i);
      ActiveFieldInfo activeFieldInfo = columnFieldGetters.get(i);
      switch (type) {
        case (Types.CHAR):
        case (Types.VARCHAR):
          statement.setString(i + 1, ((Getter<Object, String>)activeFieldInfo.setterOrGetter).get(tuple));
          break;

        case (Types.BOOLEAN):
          statement.setBoolean(i + 1, ((GetterBoolean<Object>)activeFieldInfo.setterOrGetter).get(tuple));
          break;

        case (Types.TINYINT):
          statement.setByte(i + 1, ((PojoUtils.GetterByte<Object>)activeFieldInfo.setterOrGetter).get(tuple));
          break;

        case (Types.SMALLINT):
          statement.setShort(i + 1, ((GetterShort<Object>)activeFieldInfo.setterOrGetter).get(tuple));
          break;

        case (Types.INTEGER):
          statement.setInt(i + 1, ((GetterInt<Object>)activeFieldInfo.setterOrGetter).get(tuple));
          break;

        case (Types.BIGINT):
          statement.setLong(i + 1, ((GetterLong<Object>)activeFieldInfo.setterOrGetter).get(tuple));
          break;

        case (Types.FLOAT):
          statement.setFloat(i + 1, ((GetterFloat<Object>)activeFieldInfo.setterOrGetter).get(tuple));
          break;

        case (Types.DOUBLE):
          statement.setDouble(i + 1, ((GetterDouble<Object>)activeFieldInfo.setterOrGetter).get(tuple));
          break;

        case Types.DECIMAL:
          statement.setBigDecimal(i + 1, ((Getter<Object, BigDecimal>)activeFieldInfo.setterOrGetter).get(tuple));
          break;

        case Types.TIMESTAMP:
          statement.setTimestamp(i + 1, ((Getter<Object, Timestamp>)activeFieldInfo.setterOrGetter).get(tuple));
          break;

        case Types.TIME:
          statement.setTime(i + 1, ((Getter<Object, Time>)activeFieldInfo.setterOrGetter).get(tuple));
          break;

        case Types.DATE:
          statement.setDate(i + 1, ((Getter<Object, Date>)activeFieldInfo.setterOrGetter).get(tuple));
          break;

        default:
          handleUnknownDataType(type, tuple, activeFieldInfo);
          break;
      }
    }
  }

  @SuppressWarnings("UnusedParameters")
  protected void handleUnknownDataType(int type, Object tuple, ActiveFieldInfo activeFieldInfo)
  {
    throw new RuntimeException("unsupported data type " + type);
  }

  /**
   * A list of {@link FieldInfo}s where each item maps a column name to a pojo field name.
   */
  public List<JdbcFieldInfo> getFieldInfos()
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
  public void setFieldInfos(List<JdbcFieldInfo> fieldInfos)
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

  /**
   * Set the target table name in database
   * @param tablename : table name as it is stored in the database
   */
  public void setTablename(String tablename)
  {
    this.tablename = tablename;
  }

  private static final Logger LOG = LoggerFactory.getLogger(AbstractJdbcPOJOOutputOperator.class);

  @Override
  public void activate(OperatorContext context)
  {
    super.activate(context);
    final int size = columnDataTypes.size();
    for (int i = 0; i < size; i++) {
      final int type = columnDataTypes.get(i);
      ActiveFieldInfo activeFieldInfo = columnFieldGetters.get(i);
      switch (type) {
        case (Types.CHAR):
        case (Types.VARCHAR):
          activeFieldInfo.setterOrGetter = PojoUtils.createGetter(pojoClass,
              activeFieldInfo.fieldInfo.getPojoFieldExpression(),
            String.class);
          break;

        case (Types.BOOLEAN):
          activeFieldInfo.setterOrGetter = PojoUtils.createGetterBoolean(pojoClass,
              activeFieldInfo.fieldInfo.getPojoFieldExpression());
          break;

        case (Types.TINYINT):
          activeFieldInfo.setterOrGetter = PojoUtils.createGetterByte(pojoClass,
              activeFieldInfo.fieldInfo.getPojoFieldExpression());
          break;

        case (Types.SMALLINT):
          activeFieldInfo.setterOrGetter = PojoUtils.createGetterShort(pojoClass,
              activeFieldInfo.fieldInfo.getPojoFieldExpression());
          break;

        case (Types.INTEGER):
          activeFieldInfo.setterOrGetter = PojoUtils.createGetterInt(pojoClass,
              activeFieldInfo.fieldInfo.getPojoFieldExpression());
          break;

        case (Types.BIGINT):
          activeFieldInfo.setterOrGetter = PojoUtils.createGetterLong(pojoClass,
              activeFieldInfo.fieldInfo.getPojoFieldExpression());
          break;

        case (Types.FLOAT):
          activeFieldInfo.setterOrGetter = PojoUtils.createGetterFloat(pojoClass,
              activeFieldInfo.fieldInfo.getPojoFieldExpression());
          break;

        case (Types.DOUBLE):
          activeFieldInfo.setterOrGetter = PojoUtils.createGetterDouble(pojoClass,
              activeFieldInfo.fieldInfo.getPojoFieldExpression());
          break;

        case Types.DECIMAL:
          activeFieldInfo.setterOrGetter = PojoUtils.createGetter(pojoClass,
              activeFieldInfo.fieldInfo.getPojoFieldExpression(), BigDecimal.class);
          break;

        case Types.TIMESTAMP:
          activeFieldInfo.setterOrGetter = PojoUtils.createGetter(pojoClass,
              activeFieldInfo.fieldInfo.getPojoFieldExpression(), Timestamp.class);
          break;

        case Types.TIME:
          activeFieldInfo.setterOrGetter = PojoUtils.createGetter(pojoClass,
              activeFieldInfo.fieldInfo.getPojoFieldExpression(), Time.class);
          break;

        case Types.DATE:
          activeFieldInfo.setterOrGetter = PojoUtils.createGetter(pojoClass,
              activeFieldInfo.fieldInfo.getPojoFieldExpression(), Date.class);
          break;

        default:
          handleUnknownDataType(type, null, activeFieldInfo);
          break;
      }
    }
  }

  /**
   * Function to initialize the list of {@link JdbcFieldInfo} from properties.xml
   * @param index
   * @param value
   */
  public void setFieldInfosItem(int index, String value)
  {
    try {
      JSONObject jo = new JSONObject(value);
      JdbcFieldInfo jdbcFieldInfo = new JdbcFieldInfo(jo.getString("columnName"), jo.getString("pojoFieldExpression"),
          FieldInfo.SupportType.valueOf(jo.getString("type")), jo.getInt("sqlType"));
      final int need = index - fieldInfos.size() + 1;
      for (int i = 0; i < need; i++) {
        fieldInfos.add(null);
      }
      fieldInfos.set(index,jdbcFieldInfo);
    } catch (Exception e) {
      throw new RuntimeException("Exception in setting JdbcFieldInfo");
    }
  }


}
