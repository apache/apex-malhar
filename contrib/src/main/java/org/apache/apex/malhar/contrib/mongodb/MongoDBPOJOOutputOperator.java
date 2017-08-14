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
package org.apache.apex.malhar.contrib.mongodb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.util.PojoUtils;
import org.apache.apex.malhar.lib.util.PojoUtils.Getter;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

import com.mongodb.BasicDBObject;

/**
 * Implementation of {@link MongoDBOutputOperator} that saves a POJO in the mongodb database. <br/>
 * <p>
 * @displayName MongoDb Output Operator
 * @category Output
 * @tags database, nosql, pojo, mongodb
 * @since 0.3.5
 */
@Evolving
public class MongoDBPOJOOutputOperator extends MongoDBOutputOperator<Object>
{
  private final transient ArrayList<Getter<Object, Object>> getterValues;
  @NotNull
  private ArrayList<String> keys;
  @NotNull
  private ArrayList<String> expressions;
  private static final Map<String, Class<?>> mapPrimitives = new HashMap<String, Class<?>>();
  private final transient Map<String, String[]> nestedKeys;
  @NotNull
  private transient ArrayList<String> tablenames;
  @NotNull
  private ArrayList<String> fieldTypes;

  /*
   * An ArrayList of expressions to get field values from POJO and populate mongodb.
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
   * ArrayList of Data Types of Keys specified by user in same order as Keys.
   * User needs to specify the java package also for non primitive data types.
   * Example: java.lang.String,int,char,java.lang.Integer,java.util.Map
   */
  public ArrayList<String> getFieldTypes()
  {
    return fieldTypes;
  }

  public void setFieldTypes(ArrayList<String> fieldTypes)
  {
    this.fieldTypes = fieldTypes;
  }

  /*
   * Keys to be stored in mongodb document.Example: Id,name,address.
   */
  public ArrayList<String> getKeys()
  {
    return keys;
  }

  public void setKeys(ArrayList<String> keys)
  {
    this.keys = keys;
  }

  /*
   * Fields can be stored in separate tables in the same database.This is the list of tables.
   */
  public ArrayList<String> getTablenames()
  {
    return tablenames;
  }

  public void setTablenames(ArrayList<String> tablenames)
  {
    this.tablenames = tablenames;
  }

  public MongoDBPOJOOutputOperator()
  {
    super();
    getterValues = new ArrayList<Getter<Object, Object>>();
    nestedKeys = new HashMap<String, String[]>();
  }

  static {
    mapPrimitives.put("boolean", boolean.class);
    mapPrimitives.put("char", char.class);
    mapPrimitives.put("short", short.class);
    mapPrimitives.put("int", int.class);
    mapPrimitives.put("long", long.class);
    mapPrimitives.put("float", float.class);
    mapPrimitives.put("double", double.class);
  }

  private void processFirstTuple(Object tuple)
  {
    ArrayList<Class<?>> classTypes = new ArrayList<Class<?>>();
    for (String fieldType: fieldTypes) {
      if (mapPrimitives.containsKey(fieldType)) {
        classTypes.add(mapPrimitives.get(fieldType));
      } else {
        try {
          classTypes.add(Class.forName(fieldType));
        } catch (ClassNotFoundException ex) {
          throw new RuntimeException(ex);
        }
      }
    }
    final Class<?> fqcn = tuple.getClass();
    for (int i = 0; i < expressions.size(); i++) {
      final String key = keys.get(i);
      getterValues.add(PojoUtils.createGetter(fqcn, expressions.get(i), classTypes.get(i)));
      if (key.contains(".")) {
        nestedKeys.put(key, key.split("[.]"));
      }
      String table = tablenames.get(i);
      if (!tableList.contains(table)) {
        tableList.add(table);
      }
    }

  }

  @Override
  public void processTuple(Object tuple)
  {
    tableToDocument.clear();
    BasicDBObject doc;
    BasicDBObject nestedDoc = null;

    if (getterValues.isEmpty()) {
      processFirstTuple(tuple);
    }

    for (int i = 0; i < keys.size(); i++) {
      String key = keys.get(i);
      String table = tablenames.get(i);
      //nested object are stored in a new document in mongo db.
      if (key.contains(".")) {
        String[] subKeys = nestedKeys.get(key);
        if (nestedDoc == null) {
          nestedDoc = new BasicDBObject();
        }
        nestedDoc.put(subKeys[1], (getterValues.get(i)).get(tuple));
        if ((doc = tableToDocument.get(table)) == null) {
          doc = new BasicDBObject();
        }
        if (doc.containsField(subKeys[0])) {
          doc.append(subKeys[0], nestedDoc);
        } else {
          doc.put(subKeys[0], nestedDoc);
        }
      } else {
        if ((doc = tableToDocument.get(table)) == null) {
          doc = new BasicDBObject();
        }
        doc.put(key, (getterValues.get(i)).get(tuple));
      }
      tableToDocument.put(table, doc);
    }
    processTupleCommon();

  }

  /*
   *This function is not required in POJO based implementation of MongoDBOutputOperator.
   */
  @Override
  public void setColumnMapping(String[] mapping)
  {
  }

}
