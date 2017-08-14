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
package org.apache.apex.malhar.contrib.cassandra;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.apex.malhar.lib.util.PojoUtils;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.TypeCodec;

/**
 * Used to manage simple data type based getters for given cassandra columns
 *
 * @since 3.6.0
 */
public class CassandraPojoUtils
{
  /**
   * Resolves a getter that can be associated with the given field name in the Pojo matching to the given
   * data type of cassandra
   * @param tuplePayloadClass The tuple class that is used to build the getter from
   * @param getterExpr The name of the field representing the getter that needs to be generated
   * @param returnDataTypeOfGetter The Data type of the cassandra column
   * @param userDefinedTypesClass A map that can provide for a UDT class given a column name
   * @return The getter object that can be used to extract the value at runtime
   */
  public static Object resolveGetterForField(Class tuplePayloadClass, String getterExpr,
      DataType returnDataTypeOfGetter, Map<String,Class> userDefinedTypesClass)
  {
    Object getter = null;
    switch (returnDataTypeOfGetter.getName()) {
      case INT:
        getter = PojoUtils.createGetterInt(tuplePayloadClass, getterExpr);
        break;
      case BIGINT:
      case COUNTER:
        getter = PojoUtils.createGetterLong(tuplePayloadClass, getterExpr);
        break;
      case ASCII:
      case TEXT:
      case VARCHAR:
        getter = PojoUtils.createGetter(tuplePayloadClass, getterExpr, String.class);
        break;
      case BOOLEAN:
        getter = PojoUtils.createGetterBoolean(tuplePayloadClass, getterExpr);
        break;
      case FLOAT:
        getter = PojoUtils.createGetterFloat(tuplePayloadClass, getterExpr);
        break;
      case DOUBLE:
        getter = PojoUtils.createGetterDouble(tuplePayloadClass, getterExpr);
        break;
      case DECIMAL:
        getter = PojoUtils.createGetter(tuplePayloadClass, getterExpr, BigDecimal.class);
        break;
      case SET:
        getter = PojoUtils.createGetter(tuplePayloadClass, getterExpr, Set.class);
        break;
      case MAP:
        getter = PojoUtils.createGetter(tuplePayloadClass, getterExpr, Map.class);
        break;
      case LIST:
        getter = PojoUtils.createGetter(tuplePayloadClass, getterExpr, List.class);
        break;
      case TIMESTAMP:
        getter = PojoUtils.createGetter(tuplePayloadClass, getterExpr, Date.class);
        break;
      case UUID:
        getter = PojoUtils.createGetter(tuplePayloadClass, getterExpr, UUID.class);
        break;
      case UDT:
        getter = PojoUtils.createGetter(tuplePayloadClass, getterExpr, userDefinedTypesClass.get(getterExpr));
        break;
      default:
        getter = PojoUtils.createGetter(tuplePayloadClass, getterExpr, Object.class);
        break;
    }
    return getter;
  }

  /**
   * Populates a given bound statement column with a value give a POJO and the map representing the getters
   * @param boundStatement The statement that needs to be populated with the value
   * @param getters A map mapping the applicable getter for a given column name as key
   * @param dataType The data type of the cassandra column name
   * @param cassandraColName The name of the cassandra column
   * @param pojoPayload The POJO from which the value needs to be extracted
   * @param setNulls Whether nulls can be set explicitly
   * @param codecsForCassandraColumnNames A map giving column name to codec with key as column name
   */
  @SuppressWarnings(value = "unchecked")
  public static void populateBoundStatementWithValue(BoundStatement boundStatement, Map<String,Object> getters,
      DataType dataType, String cassandraColName, Object pojoPayload, boolean setNulls,
      Map<String,TypeCodec> codecsForCassandraColumnNames)
  {
    switch (dataType.getName()) {
      case BOOLEAN:
        PojoUtils.GetterBoolean<Object> boolGetter = ((PojoUtils.GetterBoolean<Object>)getters
            .get(cassandraColName));
        if (boolGetter != null) {
          final boolean bool = boolGetter.get(pojoPayload);
          boundStatement.setBool(cassandraColName, bool);
        } else {
          boundStatement.unset(cassandraColName);
        }
        break;
      case INT:
        PojoUtils.GetterInt<Object> inGetter = ((PojoUtils.GetterInt<Object>)getters.get(cassandraColName));
        if (inGetter != null) {
          final int intValue = inGetter.get(pojoPayload);
          boundStatement.setInt(cassandraColName, intValue);
        } else {
          boundStatement.unset(cassandraColName);
        }
        break;
      case BIGINT:
      case COUNTER:
        PojoUtils.GetterLong<Object> longGetter = ((PojoUtils.GetterLong<Object>)getters.get(cassandraColName));
        if (longGetter != null) {
          final long longValue = longGetter.get(pojoPayload);
          boundStatement.setLong(cassandraColName, longValue);
        } else {
          boundStatement.unset(cassandraColName);
        }
        break;
      case FLOAT:
        PojoUtils.GetterFloat<Object> floatGetter = ((PojoUtils.GetterFloat<Object>)getters.get(cassandraColName));
        if (floatGetter != null) {
          final float floatValue = floatGetter.get(pojoPayload);
          boundStatement.setFloat(cassandraColName, floatValue);
        } else {
          boundStatement.unset(cassandraColName);
        }
        break;
      case DOUBLE:
        PojoUtils.GetterDouble<Object> doubleGetter = ((PojoUtils.GetterDouble<Object>)getters
            .get(cassandraColName));
        if (doubleGetter != null) {
          final double doubleValue = doubleGetter.get(pojoPayload);
          boundStatement.setDouble(cassandraColName, doubleValue);
        } else {
          boundStatement.unset(cassandraColName);
        }
        break;
      case DECIMAL:
        PojoUtils.Getter<Object, BigDecimal> bigDecimalGetter = ((PojoUtils.Getter<Object, BigDecimal>)getters
            .get(cassandraColName));
        if (bigDecimalGetter != null) {
          final BigDecimal decimal = bigDecimalGetter.get(pojoPayload);
          if (decimal == null) {
            if (!setNulls) {
              boundStatement.unset(cassandraColName);
            } else {
              boundStatement.setDecimal(cassandraColName, null);
            }
          } else {
            boundStatement.setDecimal(cassandraColName, decimal);
          }
        } else {
          boundStatement.unset(cassandraColName);
        }
        break;
      case UUID:
        PojoUtils.Getter<Object, UUID> uuidGetter = ((PojoUtils.Getter<Object, UUID>)getters.get(cassandraColName));
        if (uuidGetter != null) {
          final UUID uuid = uuidGetter.get(pojoPayload);
          if (uuid == null) {
            if (!setNulls) {
              boundStatement.unset(cassandraColName);
            } else {
              boundStatement.setUUID(cassandraColName, null);
            }
          } else {
            boundStatement.setUUID(cassandraColName, uuid);
          }
        } else {
          boundStatement.unset(cassandraColName);
        }
        break;
      case ASCII:
      case VARCHAR:
      case TEXT:
        PojoUtils.Getter<Object, String> stringGetter = ((PojoUtils.Getter<Object, String>)getters
            .get(cassandraColName));
        if (stringGetter != null) {
          final String ascii = stringGetter.get(pojoPayload);
          if (ascii == null) {
            if (!setNulls) {
              boundStatement.unset(cassandraColName);
            } else {
              boundStatement.setString(cassandraColName, null);
            }
          } else {
            boundStatement.setString(cassandraColName, ascii);
          }
        } else {
          boundStatement.unset(cassandraColName);
        }
        break;
      case SET:
        PojoUtils.Getter<Object, Set<?>> getterForSet = ((PojoUtils.Getter<Object, Set<?>>)getters
            .get(cassandraColName));
        if (getterForSet != null) {
          final Set<?> set = getterForSet.get(pojoPayload);
          if (set == null) {
            if (!setNulls) {
              boundStatement.unset(cassandraColName);
            } else {
              boundStatement.setSet(cassandraColName, null);
            }
          } else {
            boundStatement.setSet(cassandraColName, set);
          }
        } else {
          boundStatement.unset(cassandraColName);
        }
        break;
      case MAP:
        PojoUtils.Getter<Object, Map<?, ?>> mapGetter = ((PojoUtils.Getter<Object, Map<?, ?>>)getters
            .get(cassandraColName));
        if (mapGetter != null) {
          final Map<?, ?> map = mapGetter.get(pojoPayload);
          if (map == null) {
            if (!setNulls) {
              boundStatement.unset(cassandraColName);
            } else {
              boundStatement.setMap(cassandraColName, null);
            }
          } else {
            boundStatement.setMap(cassandraColName, map);
          }
        } else {
          boundStatement.unset(cassandraColName);
        }
        break;
      case LIST:
        PojoUtils.Getter<Object, List<?>> listGetter = ((PojoUtils.Getter<Object, List<?>>)getters
            .get(cassandraColName));
        if (listGetter != null) {
          final List<?> list = listGetter.get(pojoPayload);
          if (list == null) {
            if (!setNulls) {
              boundStatement.unset(cassandraColName);
            } else {
              boundStatement.setList(cassandraColName, null);
            }
          } else {
            boundStatement.setList(cassandraColName, list);
          }
        } else {
          boundStatement.unset(cassandraColName);
        }
        break;
      case TIMESTAMP:
        PojoUtils.Getter<Object, Date> dateGetter = ((PojoUtils.Getter<Object, Date>)getters.get(cassandraColName));
        if (dateGetter != null) {
          final Date date = dateGetter.get(pojoPayload);
          if (date == null) {
            if (!setNulls) {
              boundStatement.unset(cassandraColName);
            } else {
              boundStatement.setMap(cassandraColName, null);
            }
          } else {
            boundStatement.setDate(cassandraColName, LocalDate.fromMillisSinceEpoch(date.getTime()));
          }
        } else {
          boundStatement.unset(cassandraColName);
        }
        break;
      case UDT:
        PojoUtils.Getter<Object, Object> udtGetter = ((PojoUtils.Getter<Object, Object>)getters
            .get(cassandraColName));
        if (udtGetter != null) {
          final Object udtPayload = udtGetter.get(pojoPayload);
          if (udtPayload == null) {
            if (!setNulls) {
              boundStatement.unset(cassandraColName);
            } else {
              boundStatement.setUDTValue(cassandraColName, null);
            }
          } else {
            boundStatement.set(cassandraColName, udtPayload, codecsForCassandraColumnNames
                .get(cassandraColName).getJavaType().getRawType());
          }
        } else {
          boundStatement.unset(cassandraColName);
        }
        break;
      default:
        throw new RuntimeException("Type not supported for " + dataType.getName());
    }
  }
}

