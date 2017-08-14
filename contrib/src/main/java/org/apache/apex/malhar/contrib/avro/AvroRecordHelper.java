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
package org.apache.apex.malhar.contrib.avro;

import java.text.ParseException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * This is an utility class for reading Avro converted records.<br>
 * This class can be used with the {@link PojoToAvro} or in isolation to get Avro values.
 *
 * @since 3.4.0
 */
public class AvroRecordHelper
{

  /**
   * Convert a passed String value to the given type for the key as per Schema
   */
  public static Object convertValueStringToAvroKeyType(Schema schema, String key, String value) throws ParseException
  {
    Type type = null;

    if (schema.getField(key) != null) {
      type = schema.getField(key).schema().getType();
    } else {
      return value;
    }

    Object convertedValue = null;

    if (type == Type.UNION) {
      convertedValue = convertAndResolveUnionToPrimitive(schema, key, value);
    } else {
      convertedValue = convertValueToAvroPrimitive(type, key, value);
    }

    return convertedValue;

  }

  private static Object convertValueToAvroPrimitive(Type type, String key, String value) throws ParseException
  {
    Object newValue = value;
    switch (type) {
      case BOOLEAN:
        newValue = Boolean.parseBoolean(value);
        break;
      case DOUBLE:
        newValue = Double.parseDouble(value);
        break;
      case FLOAT:
        newValue = Float.parseFloat(value);
        break;
      case INT:
        newValue = Integer.parseInt(value);
        break;
      case LONG:
        newValue = Long.parseLong(value);
        break;
      case BYTES:
        newValue = value.getBytes();
        break;
      case STRING:
        newValue = value;
        break;
      case NULL:
        newValue = null;
        break;
      default:
        newValue = value;
    }
    return newValue;
  }

  private static Object convertAndResolveUnionToPrimitive(Schema schema, String key, String value) throws ParseException
  {
    Schema unionSchema = schema.getField(key).schema();
    List<Schema> types = unionSchema.getTypes();
    Object convertedValue = null;
    for (int i = 0; i < types.size(); i++) {
      try {
        if (types.get(i).getType() == Type.NULL) {
          if (value == null || value.equals("null")) {
            convertedValue = null;
            break;
          } else {
            continue;
          }
        }
        convertedValue = convertValueToAvroPrimitive(types.get(i).getType(), key, value);
      } catch (RuntimeException e) {
        LOG.error("Could not handle schema resolution", e);
        continue;
      }
      break;
    }

    return convertedValue;
  }

  private static final Logger LOG = LoggerFactory.getLogger(AvroRecordHelper.class);
}
