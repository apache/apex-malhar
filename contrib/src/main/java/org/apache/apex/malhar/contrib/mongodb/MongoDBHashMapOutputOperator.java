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

import java.util.HashMap;
import java.util.Map;

import com.mongodb.BasicDBObject;

/**
 * This is a MongoDB Output Operator, which uses Lists to map tuples to appropriate tables and columns in MongoDB,
 * when doing writes.
 * <p>
 * <br>
 * Ports:<br>
 * <b>Input</b>: Can have one input port, derived from base class <br>
 * <b>Output</b>: no output port<br>
 * <br>
 * Properties:<br>
 * <b>propTableMap</b>:prop-Table mapping, used for insertion tuple mapping<br>
 * <b>propColumnMap</b>:prop-Column mapping, used for insertion tuple mapping<br>
 * <br>
 * Compile time checks:<br>
 * None<br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * <b>Benchmarks</b>:
 * <br>
 * </p>
 * @displayName MongoDB Hash Map Output
 * @category Output
 * @tags mongodb
 * @since 0.3.2
 */
public class MongoDBHashMapOutputOperator<T> extends MongoDBOutputOperator<HashMap<String, T>>
{
  public transient HashMap<String, String> propTableMap = new HashMap<String, String>();  // prop-table mapping for HashMap
  public transient HashMap<String, String> propColumnMap = new HashMap<String, String>();  // prop-column mapping for HashMap

  /**
   * set Column mapping information at operator creation time
   * @param mapping
   */
  @Override
  public void setColumnMapping(String[] mapping)
  {
    for (String str : mapping) {
      String[] tokens = str.split("[:]");
      String[] subtok = tokens[1].split("[.]");
      String table = subtok[0];
      String column = subtok[1];
      String prop = tokens[0];
      String type = tokens[2];
      if (tableList.contains(table) == false) {
        tableList.add(table);
      }
      propTableMap.put(prop, table);
      propColumnMap.put(prop, column);
    }
  }

  /**
   * process tuple based on HashMap tuple
   * @param tuple
   */
  @Override
  public void processTuple(HashMap<String, T> tuple)
  {
    tableToDocument.clear();
    BasicDBObject doc = null;
    for (Map.Entry<String, T> entry : tuple.entrySet()) {
      String prop = entry.getKey();
      String table = propTableMap.get(prop);
      String column = propColumnMap.get(prop);
      if ((doc = tableToDocument.get(table)) == null) {
        doc = new BasicDBObject();
        doc.put(column, entry.getValue());
      } else {
        doc.put(column, entry.getValue());
      }
      tableToDocument.put(table, doc);
    }

    processTupleCommon();
  }
}
