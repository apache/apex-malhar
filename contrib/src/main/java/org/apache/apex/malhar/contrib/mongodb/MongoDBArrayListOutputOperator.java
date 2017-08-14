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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.mongodb.BasicDBObject;

/**
 * This is a MongoDB Output Operator, which uses HashMaps to map tuples to appropriate tables and columns in MongoDB,
 * when doing writes.
 * <p>
 * <br>
 * Ports:<br>
 * <b>Input</b>: Can have one input port, derived from base class <br>
 * <b>Output</b>: no output port<br>
 * <br>
 * Properties:<br>
 * <b>columnList</b>:column List mapping, used for insertion tuple mapping <br>
 * <b>tableMapping</b>:table mapping list, used for insertion tuple mapping<br>
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
 * @displayName MongoDB Array List Output
 * @category Output
 * @tags mongodb
 *
 * @since 0.3.2
 */
public class MongoDBArrayListOutputOperator extends MongoDBOutputOperator<ArrayList<Object>>
{
  private static final Logger logger = LoggerFactory.getLogger(MongoDBArrayListOutputOperator.class);
  private transient ArrayList<String> columnList = new ArrayList<String>();
  protected transient ArrayList<String> tableMapping = new ArrayList<String>();

  /**
   * set Column mapping information at operator creating time
   *
   * @param mapping
   */
  @Override
  public void setColumnMapping(String[] mapping)
  {
    for (String str : mapping) {
      String[] tokens = str.split("[:]");
      String[] subtok = tokens[0].split("[.]");
      String table = subtok[0];
      tableMapping.add(table);
      if (tableList.contains(table) == false) {
        tableList.add(table);
      }
      String column = subtok[1];
      columnList.add(column);
      String type = tokens[1];
    }
  }

  /**
   * process tuple based on ArrayList tuple
   *
   * @param tuple
   */
  @Override
  public void processTuple(ArrayList<Object> tuple)
  {
    tableToDocument.clear();
    BasicDBObject doc = null;
    for (int i = 0; i < tuple.size(); i++) {
      String table = tableMapping.get(i);
      if ((doc = tableToDocument.get(table)) == null) {
        doc = new BasicDBObject();
        doc.put(columnList.get(i), tuple.get(i));
      } else {
        doc.put(columnList.get(i), tuple.get(i));
      }
      tableToDocument.put(table, doc);
    }
    processTupleCommon();
  }
}
