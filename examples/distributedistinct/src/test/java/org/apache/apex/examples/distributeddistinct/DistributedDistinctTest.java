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
package org.apache.apex.examples.distributeddistinct;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.algo.UniqueValueCount.InternalCountOutput;

import com.datatorrent.api.Context.OperatorContext;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

import com.datatorrent.api.DAG;

/**
 * Test for {@link IntegerUniqueValueCountAppender} and {@link UniqueValueCountAppender}
 */
public class DistributedDistinctTest
{
  private static final Logger logger = LoggerFactory.getLogger(DistributedDistinctTest.class);

  private static final String APP_ID = "DistributedDistinctTest";
  private static final int OPERATOR_ID = 0;

  public static final String INMEM_DB_URL = "jdbc:hsqldb:mem:test;sql.syntax_mys=true";
  public static final String INMEM_DB_DRIVER = "org.hsqldb.jdbc.JDBCDriver";
  public static final String TABLE_NAME = "Test_Lookup_Cache";

  private static IntegerUniqueValueCountAppender valueCounter;
  private static String applicationPath;

  @Test
  public void testProcess() throws Exception
  {
    insertValues();
    Statement stmt = valueCounter.getStore().getConnection().createStatement();

    ResultSet resultSet = stmt.executeQuery("SELECT col2 FROM " + TABLE_NAME + " WHERE col1 = 1");
    ArrayList<Integer> answersOne = new ArrayList<Integer>();
    for (int i = 1; i < 16; i++) {
      answersOne.add(i);
    }
    Assert.assertEquals(answersOne, processResult(resultSet));

    resultSet = stmt.executeQuery("SELECT col2 FROM " + TABLE_NAME + " WHERE col1 = 2");
    ArrayList<Integer> answersTwo = new ArrayList<Integer>();
    answersTwo.add(3);
    answersTwo.add(6);
    answersTwo.add(9);
    for (int i = 11; i < 21; i++) {
      answersTwo.add(i);
    }
    Assert.assertEquals(answersTwo, processResult(resultSet));

    resultSet = stmt.executeQuery("SELECT col2 FROM " + TABLE_NAME + " WHERE col1 = 3");
    ArrayList<Integer> answersThree = new ArrayList<Integer>();
    answersThree.add(2);
    answersThree.add(4);
    answersThree.add(6);
    answersThree.add(8);
    answersThree.add(10);
    for (int i = 11; i < 21; i++) {
      answersThree.add(i);
    }
    Assert.assertEquals(answersThree, processResult(resultSet));

    valueCounter.teardown();
  }

  public static void insertValues()
  {
    logger.debug("start round 0");
    valueCounter.beginWindow(0);
    emitKeyVals(1, 1, 10, 1);
    emitKeyVals(1, 5, 15, 1);
    valueCounter.endWindow();
    logger.debug("end round 0");

    logger.debug("start round 1");
    valueCounter.beginWindow(1);
    emitKeyVals(2, 3, 15, 3);
    emitKeyVals(3, 2, 20, 2);
    emitKeyVals(3, 11, 20, 1);
    valueCounter.endWindow();
    logger.debug("end round 1");

    logger.debug("start round 2");
    valueCounter.beginWindow(2);
    emitKeyVals(3, 2, 20, 2);
    emitKeyVals(2, 11, 20, 1);
    valueCounter.endWindow();
    logger.debug("end round 2");
  }

  public static ArrayList<Integer> processResult(ResultSet resultSet)
  {
    ArrayList<Integer> tempList = new ArrayList<Integer>();
    try {
      while (resultSet.next()) {
        tempList.add(resultSet.getInt(1));
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    Collections.sort(tempList);
    return tempList;
  }

  public static void emitKeyVals(int key, int start, int end, int increment)
  {
    int count = 0;
    Set<Object> valSet = new HashSet<Object>();
    for (int i = start; i <= end; i += increment) {
      count++;
      valSet.add(i);
    }
    valueCounter.processTuple(new InternalCountOutput<Integer>(key, count, valSet));
  }

  @Test
  public void testSetup() throws Exception
  {
    insertValues();
    Statement stmt = valueCounter.getStore().getConnection().createStatement();
    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributes = new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_ID, APP_ID);
    attributes.put(DAG.APPLICATION_PATH, applicationPath);
    attributes.put(OperatorContext.ACTIVATION_WINDOW_ID, 2L);

    valueCounter.setup(mockOperatorContext(0, attributes));

    ResultSet resultSet = stmt.executeQuery("SELECT col2 FROM " + TABLE_NAME + " WHERE col1 = 2");
    ArrayList<Integer> answersAfterClear = new ArrayList<Integer>();
    for (int i = 3; i < 16; i += 3) {
      answersAfterClear.add(i);
    }
    Assert.assertEquals(answersAfterClear, processResult(resultSet));

    resultSet = stmt.executeQuery("SELECT col2 FROM " + TABLE_NAME + " WHERE col1 = 3");
    ArrayList<Integer> answersThree = new ArrayList<Integer>();
    answersThree.add(2);
    answersThree.add(4);
    answersThree.add(6);
    answersThree.add(8);
    answersThree.add(10);
    for (int i = 11; i < 21; i++) {
      answersThree.add(i);
    }
    Assert.assertEquals(answersThree, processResult(resultSet));
    stmt.executeQuery("DELETE FROM " + TABLE_NAME);
  }

  @BeforeClass
  public static void setup() throws Exception
  {
    valueCounter = new IntegerUniqueValueCountAppender();
    Class.forName(INMEM_DB_DRIVER).newInstance();
    Connection con = DriverManager.getConnection(INMEM_DB_URL, new Properties());
    Statement stmt = con.createStatement();
    stmt.execute("CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (col1 INTEGER, col2 INTEGER, col3 BIGINT)");
    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributes = new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_ID, APP_ID);
    attributes.put(DAG.APPLICATION_PATH, applicationPath);
    attributes.put(OperatorContext.ACTIVATION_WINDOW_ID, 0L);
    valueCounter.setTableName(TABLE_NAME);
    valueCounter.getStore().setDatabaseDriver(INMEM_DB_DRIVER);
    valueCounter.getStore().setDatabaseUrl(INMEM_DB_URL);
    OperatorContext context = mockOperatorContext(OPERATOR_ID, attributes);
    valueCounter.setup(context);
  }
}
