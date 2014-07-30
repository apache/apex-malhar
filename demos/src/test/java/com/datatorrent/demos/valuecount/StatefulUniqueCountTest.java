/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.valuecount;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.demos.distributeddistinct.IntegerUniqueValueCountAppender;
import com.datatorrent.lib.algo.UniqueValueCount;
import com.datatorrent.lib.util.KeyValPair;

public class StatefulUniqueCountTest
{

  static class KeyGen implements InputOperator
  {
    public transient DefaultOutputPort<KeyValPair<Integer, Object>> output = new DefaultOutputPort<KeyValPair<Integer, Object>>();

    @Override
    public void beginWindow(long windowId)
    {
    }

    public void emitKeyVals(int key, int start, int end, int increment)
    {
      for (int i = start; i <= end; i += increment) {
        output.emit(new KeyValPair<Integer, Object>(key, i));
      }
    }

    @Override
    public void endWindow()
    {
    }

    @Override
    public void setup(OperatorContext context)
    {

    }

    @Override
    public void teardown()
    {

    }

    @Override
    public void emitTuples()
    {
      emitKeyVals(1, 1, 10, 1);
      emitKeyVals(2, 3, 15, 3);
      emitKeyVals(3, 2, 20, 2);
      emitKeyVals(1, 5, 15, 1);
      emitKeyVals(2, 11, 20, 1);
      emitKeyVals(3, 11, 20, 1);
    }
  }

  static class VerifyTable extends BaseOperator
  {

    private static final String INMEM_DB_URL = "jdbc:hsqldb:mem:test;sql.syntax_mys=true";
    private static final String INMEM_DB_DRIVER = "org.hsqldb.jdbc.JDBCDriver";
    protected static final String TABLE_NAME = "Test_Lookup_Cache";

    public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>() {
      @Override
      public void process(Object tuple)
      {
      }
    };

    @Override
    public void beginWindow(long windowId)
    {
    }

    @Override
    public void endWindow()
    {
      try {
        Class.forName(INMEM_DB_DRIVER).newInstance();
        Connection con = DriverManager.getConnection(INMEM_DB_URL, new Properties());
        Statement stmt = con.createStatement();
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
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void setup(OperatorContext context)
    {
    }

    @Override
    public void teardown()
    {

    }

    public static ArrayList<Integer> processResult(ResultSet resultSet)
    {
      ArrayList<Integer> tempList = new ArrayList<Integer>();
      try {
        while (resultSet.next()) {
          tempList.add(resultSet.getInt(1));
        }
      } catch (SQLException e) {
        e.printStackTrace();
      }
      Collections.sort(tempList);
      return tempList;
    }
  }

  public class Application implements StreamingApplication
  {
    @SuppressWarnings("unchecked")
    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      KeyGen keyGen = dag.addOperator("KeyGenerator", new KeyGen());
      UniqueValueCount<Integer> valCount = dag.addOperator("ValueCounter", new UniqueValueCount<Integer>());
      IntegerUniqueValueCountAppender uniqueUnifier = dag.addOperator("Unique", new IntegerUniqueValueCountAppender());
      VerifyTable verifyTable = dag.addOperator("VerifyTable", new VerifyTable());

      @SuppressWarnings("rawtypes")
      DefaultOutputPort valOut = valCount.output;
      @SuppressWarnings("rawtypes")
      DefaultOutputPort uniqueOut = uniqueUnifier.output;

      dag.addStream("DataIn", keyGen.output, valCount.input);
      dag.addStream("UnifyWindows", valOut, uniqueUnifier.input);
      dag.addStream("ResultsOut", uniqueOut, verifyTable.input);
    }
  }
  
  @Test
  public void testSomeMethod() throws Exception
  {
    LocalMode.runApp(new Application(), 5000);
  }
}
