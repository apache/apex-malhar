package com.datatorrent.contrib.couchbase;

import com.datatorrent.api.AttributeMap;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * *
 * * @author prerna
 * */
public class CouchBaseOperatorTest {

private static final Logger logger = LoggerFactory.getLogger(CouchBaseOperatorTest.class);
private static String APP_ID = "CouchBaseOperatorTest";
private static String bucket = "default";
private static String password = "";
private static int OPERATOR_ID = 0;
protected static ArrayList<URI> nodes = new ArrayList<URI>();
protected static ArrayList<String> keyList = new ArrayList<String>();
private static String uri = "http://node26.morado.com:8091/pools";

private static class TestEvent {

String key;
Integer value;

TestEvent(String key, int val) {
this.key = key;
this.value = value;
}
}

public CouchBaseOperatorTest() {
}

@BeforeClass
public static void setUpClass() {
}

@AfterClass
public static void tearDownClass() {
}

@Before
public void setUp() {

}

@After
public void tearDown() {
}

private static void setupConnection() {
CouchBaseStore store = new CouchBaseStore();
CouchBaseWindowStore metaStore = new CouchBaseWindowStore();
try {
store.connect();
metaStore.connect();
} catch (IOException ex) {
java.util.logging.Logger.getLogger(CouchBaseTest.class.getName()).log(Level.SEVERE, null, ex);
}

}

@Test
public void TestCouchBaseInsertOutputOperator() {

CouchBaseWindowStore store = new CouchBaseWindowStore();
store.setBucket("default");
store.setPassword("");
store.setUriString("node26.morado.com:8091");
try {
store.connect();
} catch (IOException ex) {
java.util.logging.Logger.getLogger(CouchBaseOperatorTest.class.getName()).log(Level.SEVERE, null, ex);
}

TestInsertOutputOperator outputInsertOperator = new TestInsertOutputOperator();
AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
attributeMap.put(DAG.APPLICATION_ID, APP_ID);
OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

outputInsertOperator.setStore(store);

outputInsertOperator.setup(context);


long startTime = System.currentTimeMillis();
System.out.println("start time is " + startTime);
for (int i = 0; i < 1000000; i++) {
outputInsertOperator.beginWindow(0);
    outputInsertOperator.input.process(new TestEvent("TEST" + i, i));
outputInsertOperator.endWindow();
}
long stopTime = System.currentTimeMillis();
System.out.println("stop time is " + stopTime);
long elapsedTime = stopTime - startTime;
System.out.println("total time is " + elapsedTime);

}

@Test
public void TestCouchBaseUpdateOutputOperator() {

CouchBaseWindowStore store = new CouchBaseWindowStore();

try {
store.connect();
} catch (IOException ex) {
java.util.logging.Logger.getLogger(CouchBaseOperatorTest.class.getName()).log(Level.SEVERE, null, ex);
}

AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
attributeMap.put(DAG.APPLICATION_ID, APP_ID);
OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

TestUpdateOutputOperator outputUpdateOperator = new TestUpdateOutputOperator();

outputUpdateOperator.setStore(store);

outputUpdateOperator.setup(context);


List<CouchBaseOperatorTest.TestEvent> events = Lists.newArrayList();
for (int i = 0; i < 10; i++) {
events.add(new CouchBaseOperatorTest.TestEvent("key" + i, i));
}

outputUpdateOperator.beginWindow(0);
for (TestEvent event : events) {
outputUpdateOperator.input.put(event);
}
outputUpdateOperator.endWindow();


}

@Test
public void TestCouchBaseInputOperator() {

CouchBaseStore store = new CouchBaseStore();
store.setBucket("default");
store.setPassword("");
store.setUriString("node26.morado.com:8091");
try {
store.connect();
} catch (IOException ex) {
java.util.logging.Logger.getLogger(CouchBaseOperatorTest.class.getName()).log(Level.SEVERE, null, ex);
}
AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
attributeMap.put(DAG.APPLICATION_ID, APP_ID);
OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

CouchBaseOperatorTest.TestInputOperator inputOperator = new CouchBaseOperatorTest.TestInputOperator();
inputOperator.setStore(store);
inputOperator.insertEventsInTable(100);

CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
inputOperator.outputPort.setSink(sink);

inputOperator.setup(context);
long startTime = System.currentTimeMillis();
inputOperator.beginWindow(0);
for (int i = 0; i < 1000000; i++) {
inputOperator.emitTuples();
}
inputOperator.endWindow();
long stopTime = System.currentTimeMillis();

long elapsedTime = stopTime - startTime;
System.out.println("total time is " + elapsedTime);
//sink.clear();
//Assert.assertEquals("rows from db", 10, sink.collectedTuples.size());
}


public static class TestInputOperator extends AbstractCouchBaseInputOperator<String> {


@SuppressWarnings("unchecked")
@Override
public String getTuple(Object entry) {
String tuple = entry.toString();
return tuple;
}

@Override
public ArrayList<String> getKeys() {
return keyList;
}

private void insertEventsInTable(int numEvents) {
String key = null;
Integer value = null;

for (int i = 0; i < numEvents; i++) {
    key = String.valueOf("Key" + i*10);
    keyList.add(key);
    value = i*100;
    try {
        store.client.set(key, value).get();
    } catch (InterruptedException ex) {
        java.util.logging.Logger.getLogger(CouchBaseOperatorTest.class.getName()).log(Level.SEVERE, null, ex);
    } catch (ExecutionException ex) {
        java.util.logging.Logger.getLogger(CouchBaseOperatorTest.class.getName()).log(Level.SEVERE, null, ex);
    }
}
}

}

private static class TestInsertOutputOperator extends AbstractInsertCouchBaseOutputOperator<TestEvent> {

public int getNumOfEventsInStore() {


Map<String, Object> keyValues = store.client.getBulk(keyList);
System.out.println("keyValues is" + keyValues.toString());
System.out.println("size is " + keyValues.size());
java.util.logging.Logger.getLogger(CouchBaseOperatorTest.class.getName()).log(Level.SEVERE, keyValues.toString());

return keyValues.size();
}

@Override
public String generatekey(TestEvent tuple) {
tuple.key = "new";
return tuple.key;

}

@Override
public Object getObject(TestEvent tuple) {
tuple.value = 20;
return tuple.value;
}

}

private static class TestUpdateOutputOperator extends AbstractUpdateCouchBaseOutputOperator<TestEvent> {

TestUpdateOutputOperator() {
setupConnection();
}

public int getNumOfEventsInStore() {

Map<String, Object> keyValues = store.client.getBulk(keyList);
System.out.println("keyValues is" + keyValues.toString());
java.util.logging.Logger.getLogger(CouchBaseOperatorTest.class.getName()).log(Level.SEVERE, keyValues.toString());
return keyValues.size();
}

@Override
public String generatekey(TestEvent tuple) {
tuple.key = "update";
return tuple.key;

}

@Override
public Object getObject(TestEvent tuple) {
tuple.value = 100;
return tuple.value;
}

}
}
