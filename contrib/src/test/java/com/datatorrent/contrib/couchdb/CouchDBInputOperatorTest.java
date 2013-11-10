package com.datatorrent.contrib.couchdb;

import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.google.common.collect.Maps;
import junit.framework.Assert;
import org.ektorp.ViewQuery;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @since 0.3.5
 */
public class CouchDBInputOperatorTest
{
  private static Logger LOG = LoggerFactory.getLogger(CouchDBInputOperatorTest.class);

  private class TestMapBasedCouchInputOperatorTest extends AbstractMapBasedInputOperator
  {
    @Override
    public ViewQuery getViewQuery()
    {
      return CouchDBTestHelper.get().createAndFetchViewQuery();
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void testCouchDBInputOperator()
  {
    String testDocumentId = "TestDocument1";
    Map<String, String> mapTuple = Maps.newHashMap();
    mapTuple.put("_id", testDocumentId);
    mapTuple.put("name", "TD1");
    mapTuple.put("type", "test");
    CouchDBTestHelper.get().insertDocument(mapTuple);

    TestMapBasedCouchInputOperatorTest operatorTest = new TestMapBasedCouchInputOperatorTest();
    CollectorTestSink sink = new CollectorTestSink();
    operatorTest.outputPort.setSink(sink);
    operatorTest.setDatabase(CouchDBTestHelper.get().getDatabase());
    operatorTest.setup(new OperatorContextTestHelper.TestIdOperatorContext(2));

    operatorTest.beginWindow(0);
    operatorTest.emitTuples();
    operatorTest.endWindow();
    Assert.assertTrue("number emitted tuples", sink.collectedTuples.size() > 0);

    boolean found = false;
    for (Object o : sink.collectedTuples) {
      LOG.debug(o.toString());
      Map<Object, Object> document = (Map<Object, Object>) o;
      if (document.get("_id").equals(testDocumentId)) {
        found = true;
        Assert.assertEquals("name in document", "TD1", document.get("name"));
        Assert.assertEquals("type of ducement", "test", document.get("type"));
      }
    }
    Assert.assertTrue("inserted tuple was found", found);
  }
}
