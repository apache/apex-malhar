package com.datatorrent.contrib.couchbase;

import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.DAG;
import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.junit.*;
import org.python.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datatorrent.contrib.couchbase.AbstractCouchBaseOutputOperator.CouchBaseJSONSerializer;


public class CouchBaseOperatorTest
{

  private static final Logger logger = LoggerFactory.getLogger(CouchBaseOperatorTest.class);
  private static String APP_ID = "CouchBaseOperatorTest";
  private static String bucket = "new";
  private static String password = "";
  private static int OPERATOR_ID = 0;
  protected static ArrayList<URI> nodes = new ArrayList<URI>();
  protected static ArrayList<String> keyList;
  private static String uri = "node13.morado.com:8091,node14.morado.com:8091";

  private static class TestEvent implements Serializable
  {

    String key;
    Integer value;

    TestEvent(String key, int val)
    {
      this.key = key;
      this.value = value;
    }

  }

  @Test
  public void TestCouchBaseOutputOperator()
  {

    CouchBaseWindowStore store = new CouchBaseWindowStore();
    store.setBucket(bucket);
    store.setPassword(password);
    store.setUriString(uri);
    keyList = new ArrayList<String>();
    try {
      store.connect();
    }
    catch (IOException ex) {
      DTThrowable.rethrow(ex);
    }
    store.getInstance().flush();
    store.getMetaInstance().flush();
    CouchBaseOutputOperator outputOperator = new CouchBaseOutputOperator();
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

    outputOperator.setStore(store);

    outputOperator.setup(context);
    CouchBaseJSONSerializer serializer = new CouchBaseJSONSerializer();
    outputOperator.setSerializer(serializer);
    List<TestEvent> events = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      events.add(new TestEvent("key" + i, i));
      keyList.add("key" + i);
    }
    
   logger.info("keylist is " + keyList.toString());
    outputOperator.beginWindow(0);
logger.info("after begin window");
    for (TestEvent event: events) {
logger.info("before process window");
      outputOperator.generateKey(event);
      outputOperator.getValue(event);
      outputOperator.input.process(event);
logger.info("after process window");
    }
logger.info("before endwindow");
    outputOperator.endWindow();
logger.info("after endwindow");
     Map<String, Object> keyValues = store.getInstance().getBulk(keyList);
      logger.info("keyValues is" + keyValues.toString());
      logger.info("size is " + keyValues.size());
    int k = outputOperator.getNumOfEventsInStore();
    Assert.assertEquals("rows in db", 10, keyValues.size());

  }
  
  
  public void TestCouchBaseUpdateOutputOperator()
  {
    CouchBaseWindowStore store = new CouchBaseWindowStore();

    store.setBucket(bucket);
    store.setPassword(password);

    store.setUriString(uri);
    try {
      store.connect();
    }
    catch (IOException ex) {
      DTThrowable.rethrow(ex);
    }
    store.getInstance().flush();
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

    CouchBaseUpdateOperator updateOperator = new CouchBaseUpdateOperator();
    keyList = new ArrayList<String>();
    updateOperator.setStore(store);

    updateOperator.setup(context);

    List<TestEvent> events = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      events.add(new TestEvent("key" + i, i));
      keyList.add("key" + i);
    }

    updateOperator.beginWindow(0);
    for (TestEvent event: events) {
      updateOperator.generateKey(event);
      updateOperator.getValue(event);
      updateOperator.input.process(event);
    }
    updateOperator.endWindow();
    Assert.assertEquals("rows in db", 10, updateOperator.getNumOfEventsInStore());
  }

  public void TestCouchBaseInputOperator()
  {
    CouchBaseWindowStore store = new CouchBaseWindowStore();
    store.setBucket(bucket);
    store.setPassword(password);
    store.setUriString(uri);
    try {
      store.connect();
    }
    catch (IOException ex) {
      DTThrowable.rethrow(ex);
    }

    store.getInstance().flush();
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

    CouchBaseOperatorTest.TestInputOperator inputOperator = new CouchBaseOperatorTest.TestInputOperator();
    inputOperator.setStore(store);
    inputOperator.insertEventsInTable(100);

    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    inputOperator.outputPort.setSink(sink);

    inputOperator.setup(context);
    inputOperator.beginWindow(0);
    inputOperator.emitTuples();
    inputOperator.endWindow();

    Assert.assertEquals("tuples in couchbase", 100, sink.collectedTuples.size());
  }

  public static class TestInputOperator extends AbstractCouchBaseInputOperator<String>
  {

    @SuppressWarnings("unchecked")
    @Override
    public String getTuple(Object entry)
    {
      String tuple = entry.toString();
      return tuple;
    }

    @Override
    public ArrayList<String> getKeys()
    {
      return keyList;
    }

    private void insertEventsInTable(int numEvents)
    {
      String key = null;
      Integer value = null;
      logger.info("number of events is" + numEvents);
      for (int i = 0; i < numEvents; i++) {
        key = String.valueOf("Key" + i * 10);
        keyList.add(key);
        value = i * 100;
        try {
          store.client.set(key, value).get();
        }
        catch (InterruptedException ex) {
          DTThrowable.rethrow(ex);
        }
        catch (ExecutionException ex) {
          DTThrowable.rethrow(ex);
        }
      }
    }

  }

  private static class CouchBaseOutputOperator extends AbstractInsertCouchBaseOutputOperator<TestEvent>
  {

    public int getNumOfEventsInStore()
    {
      Map<String, Object> keyValues = store.client.getBulk(keyList);
      logger.info("keyValues is" + keyValues.toString());
      logger.info("size is " + keyValues.size());
      return keyValues.size();
    }

    @Override
    public String generateKey(TestEvent tuple)
    {
      return tuple.key;
    }

    @Override
    public Object getValue(TestEvent tuple)
    {
      tuple.value = 20;
      return tuple.value;
    }
  }

  private static class CouchBaseUpdateOperator extends AbstractUpdateCouchBaseOutputOperator<TestEvent>
  {

    public int getNumOfEventsInStore()
    {
      Map<String, Object> keyValues = store.client.getBulk(keyList);
      logger.info("keyValues is" + keyValues.toString());
      return keyValues.size();
    }

    @Override
    public String generateKey(TestEvent tuple)
    {
      return tuple.key;
    }

    @Override
    public Object getValue(TestEvent tuple)
    {
      tuple.value = 100;
      return tuple.value;
    }

  }

}



