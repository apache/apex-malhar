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
import java.util.Date;
import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap;
import com.datatorrent.api.DAG;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

public class MongoDBOutputOperatorTest
{
  private static final Logger logger = LoggerFactory.getLogger(MongoDBOutputOperatorTest.class);
  public String[] hashMapping1 = new String[columnNum];
  public String[] arrayMapping1 = new String[columnNum];
  public static final int maxTuple = 20;
  public static final int columnNum = 5;
  public AttributeMap attrmap = new DefaultAttributeMap();
  private static final int inputCount = 6;

  public void buildDataset()
  {
    hashMapping1[0] = "prop1:t1.col1:INT";
    hashMapping1[1] = "prop3:t1.col3:STRING";
    hashMapping1[2] = "prop2:t1.col2:DATE";
    hashMapping1[3] = "prop4:t2.col1:STRING";
    hashMapping1[4] = "prop5:t2.col2:INT";

    arrayMapping1[0] = "t1.col1:INT";
    arrayMapping1[1] = "t1.col3:STRING";
    arrayMapping1[2] = "t1.col2:DATE";
    arrayMapping1[3] = "t2.col2:STRING";
    arrayMapping1[4] = "t2.col1:INT";

    attrmap.put(DAG.APPLICATION_ID, "myMongoDBOouputOperatorAppId");

  }

  public HashMap<String, Object> generateHashMapData(int j, MongoDBHashMapOutputOperator oper)
  {
    HashMap<String, Object> hm = new HashMap<String, Object>();
    for (int i = 0; i < columnNum; i++) {
      String[] tokens = hashMapping1[i].split("[:]");
      String[] subtok = tokens[1].split("[.]");
      String table = subtok[0];
      String prop = tokens[0];
      String type = tokens[2];
      if (type.contains("INT")) {
        hm.put(prop, j * columnNum + i);
      } else if (type.equals("STRING")) {
        hm.put(prop, String.valueOf(j * columnNum + i));
      } else if (type.equals("DATE")) {
        hm.put(prop, new Date());
      }
      oper.propTableMap.put(prop, table);
    }
    return hm;
  }

  public ArrayList<Object> generateArrayListData(int j, MongoDBArrayListOutputOperator oper)
  {
    ArrayList<Object> al = new ArrayList<Object>();
    for (int i = 0; i < columnNum; i++) {
      String[] tokens = arrayMapping1[i].split("[:]");
      String[] subtok = tokens[0].split("[.]");
      String table = subtok[0];
      String prop = subtok[1];
      String type = tokens[1];
      if (type.contains("INT")) {
        al.add(j * columnNum + i);
      } else if (type.equals("STRING")) {
        al.add(String.valueOf(j * columnNum + i));
      } else if (type.equals("DATE")) {
        al.add(new Date());
      }

    }
    return al;
  }

  public void readDB(MongoDBOutputOperator oper)
  {
    for (Object o: oper.getTableList()) {
      String table = (String)o;
      DBCursor cursor = oper.db.getCollection(table).find();
      while (cursor.hasNext()) {
        logger.debug("{}", cursor.next());
      }
    }
  }

  @Test
  public void MongoDBHashMapOutputOperatorTest()
  {
    buildDataset();

    MongoDBHashMapOutputOperator<Object> oper = new MongoDBHashMapOutputOperator<Object>();

    oper.setBatchSize(3);
    oper.setHostName("localhost");
    oper.setDataBase("test");
    oper.setUserName("test");
    oper.setPassWord("123");
    oper.setWindowIdColumnName("winid");
    oper.setOperatorIdColumnName("operatorid");
    oper.setMaxWindowTable("maxWindowTable");
    oper.setQueryFunction(1);
    oper.setColumnMapping(hashMapping1);

    oper.setup(mockOperatorContext(1));

    for (Object o: oper.getTableList()) {
      String table = (String)o;
      oper.db.getCollection(table).drop();
    }

    oper.beginWindow(oper.getLastWindowId() + 1);
    logger.debug("beginwindow {}", oper.getLastWindowId() + 1);

    for (int i = 0; i < maxTuple; ++i) {
      HashMap<String, Object> hm = generateHashMapData(i, oper);
//      logger.debug(hm.toString());
      oper.inputPort.process(hm);

    }
    oper.endWindow();
    readDB(oper);

    oper.teardown();
  }

  @Test
  public void MongoDBArrayListOutputOperatorTest()
  {
    buildDataset();
    MongoDBArrayListOutputOperator oper = new MongoDBArrayListOutputOperator();

    oper.setBatchSize(3);
    oper.setHostName("localhost");
    oper.setDataBase("test");
    oper.setUserName("test");
    oper.setPassWord("123");
    oper.setWindowIdColumnName("winid");
    oper.setOperatorIdColumnName("operatorid");
    oper.setMaxWindowTable("maxWindowTable");
    oper.setQueryFunction(1);
    oper.setColumnMapping(arrayMapping1);

    oper.setup(mockOperatorContext(2));
    for (Object o: oper.getTableList()) {
      String table = (String)o;
      oper.db.getCollection(table).drop();
    }

    oper.beginWindow(oper.getLastWindowId() + 1);
    logger.debug("beginwindow {}", oper.getLastWindowId() + 1);

    for (int i = 0; i < maxTuple; ++i) {
      ArrayList<Object> al = generateArrayListData(i, oper);
      oper.inputPort.process(al);

    }
    oper.endWindow();
    readDB(oper);

    oper.teardown();
  }

  @Test
  public void MongoDBPOJOOperatorTest()
  {
    buildDataset();
    MongoDBPOJOOutputOperator oper = new MongoDBPOJOOutputOperator();

    oper.setBatchSize(3);
    oper.setHostName("localhost");
    oper.setDataBase("test1");
    oper.setUserName("test");
    oper.setPassWord("123");
    oper.setWindowIdColumnName("winid");
    oper.setOperatorIdColumnName("operatorid");
    oper.setMaxWindowTable("maxWindowTable");
    oper.setQueryFunction(1);
    ArrayList<String> tablenames = new ArrayList<String>();
    for (int i = 0; i < inputCount; i++) {
      tablenames.add("t1");
    }

    oper.tableList.add("t1");

    oper.setTablenames(tablenames);
    ArrayList<String> fieldTypes = new ArrayList<String>();
    fieldTypes.add("java.lang.String");
    fieldTypes.add("java.lang.String");
    fieldTypes.add("java.lang.Integer");
    fieldTypes.add("java.util.Map");
    fieldTypes.add("java.lang.String");
    fieldTypes.add("int");
    oper.setFieldTypes(fieldTypes);
    ArrayList<String> keys = new ArrayList<String>();
    keys.add("id");
    keys.add("name");
    keys.add("age");
    keys.add("mapping");
    keys.add("address.city");
    keys.add("address.housenumber");
    oper.setKeys(keys);

    ArrayList<String> expressions = new ArrayList<String>();
    expressions.add("getId()");
    expressions.add("getName()");
    expressions.add("getAge()");
    expressions.add("getMapping()");
    expressions.add("getAddress().getCity()");
    expressions.add("getAddress().getHousenumber()");
    oper.setExpressions(expressions);
    oper.setup(mockOperatorContext(2));
    for (String table: oper.getTableList()) {
      logger.debug("table in test is {}", table);
      oper.db.getCollection(table).drop();
    }

    Assert.assertEquals("size of tablenames", inputCount, tablenames.size());
    Assert.assertEquals("size of tables is same as size of expressions",tablenames.size(), expressions.size());
    Assert.assertEquals("size of keys is same as size of expressions",keys.size(), expressions.size());
    Assert.assertEquals("size of fieldTypes is same as size of expressions",fieldTypes.size(), expressions.size());

    oper.beginWindow(oper.getLastWindowId() + 1);
    logger.debug("beginwindow {}", oper.getLastWindowId() + 1);
    for (int i = 0; i < 3; i++) {
      TestPOJO al = new TestPOJO();
      al.setId("test" + i);
      al.setName("testname" + i);
      al.setAge(i);
      TestPOJO.Address address = new TestPOJO.Address();
      address.setCity("testcity" + i);
      address.setHousenumber(i + 10);
      HashMap<String, Object> hmap = new HashMap<String, Object>();
      hmap.put("key" + i, i);
      al.setMapping(hmap);
      al.setAddress(address);
      oper.inputPort.process(al);
    }
    oper.endWindow();

    for (String table: oper.getTableList()) {
      DBCursor cursor = oper.db.getCollection(table).find();
      DBObject obj = cursor.next();
      Assert.assertEquals("id is ", "test0", obj.get("id"));
      Assert.assertEquals("name is", "testname0", obj.get("name"));
      Assert.assertEquals("age is ", 0, obj.get("age"));
      Assert.assertEquals("mapping is", "{ \"key0\" : 0}", obj.get("mapping").toString());
      Assert.assertEquals("address is", "{ \"city\" : \"testcity0\" , \"housenumber\" : 10}", obj.get("address").toString());
      obj = cursor.next();
      Assert.assertEquals("id is ", "test1", obj.get("id"));
      Assert.assertEquals("name is", "testname1", obj.get("name"));
      Assert.assertEquals("age is ", 1, obj.get("age"));
      Assert.assertEquals("mapping is", "{ \"key1\" : 1}", obj.get("mapping").toString());
      Assert.assertEquals("address is", "{ \"city\" : \"testcity1\" , \"housenumber\" : 11}", obj.get("address").toString());
      obj = cursor.next();
      Assert.assertEquals("id is ", "test2", obj.get("id"));
      Assert.assertEquals("name is", "testname2", obj.get("name"));
      Assert.assertEquals("age is ", 2, obj.get("age"));
      Assert.assertEquals("mapping is", "{ \"key2\" : 2}", obj.get("mapping").toString());
      Assert.assertEquals("address is", "{ \"city\" : \"testcity2\" , \"housenumber\" : 12}", obj.get("address").toString());
    }
    oper.teardown();
  }

}
