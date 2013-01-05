/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.mongodb;

import com.malhartech.bufferserver.util.Codec;
import com.malhartech.engine.TestSink;
import com.mongodb.DBCursor;
import java.util.HashMap;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class MongoDBInputOperatorTest
{
  private static final Logger logger = LoggerFactory.getLogger(MongoDBInputOperatorTest.class);
  public String[] hashMapping1 = new String[columnNum];
  public String[] arrayMapping1 = new String[columnNum];
  public final static int maxTuple = 20;
  public final static int columnNum = 5;

  public class MyMongoDBInputOperator extends MongoDBInputOperator<Object>
  {
    @Override
    public Object getTuple(DBCursor result)
    {
      while(result.hasNext()) {
        System.out.println(result.next().toString());
      }
      return result;
    }
  };

  @Test
  public void MongoDBInputOperatorTest()
  {
    MyMongoDBInputOperator oper = new MyMongoDBInputOperator();

    oper.setHostName("localhost");
    oper.setDataBase("test");
    oper.setUserName("test");
    oper.setPassWord("123");
    oper.setTable("t1");

    oper.setup(new com.malhartech.engine.OperatorContext("1", null, null));

    oper.beginWindow(0);
    
    TestSink sink = new TestSink();
    oper.outputPort.setSink(sink);

    oper.emitTuples();

    oper.endWindow();

    oper.teardown();
  }
}
