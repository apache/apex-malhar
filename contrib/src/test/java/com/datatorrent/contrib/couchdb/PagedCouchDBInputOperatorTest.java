package com.datatorrent.contrib.couchdb;

import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.google.common.collect.Maps;
import junit.framework.Assert;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.ektorp.ViewQuery;
import org.ektorp.ViewResult;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

/**
 * Test for {@link PagedCouchDBInputOperatorTest}
 * @since 0.3.5
 */
public class PagedCouchDBInputOperatorTest
{
  private class TestPagedDBInputOperator extends AbstractPagedCouchDBInputOperator<Map<Object, Object>>
  {

    @Override
    public ViewQuery getViewQuery()
    {
      return CouchDBTestHelper.get().createAndFetchViewQuery();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<Object, Object> getTuple(ViewResult.Row row)
    {
      Map<Object, Object> valueMap = Maps.newHashMap();
      try {
        valueMap = mapper.readValue(row.getValueAsNode(), valueMap.getClass());
      } catch (IOException e) {
        e.printStackTrace();
      }
      return valueMap;
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void testCouchDBInputOperator()
  {
    String testDocumentIdPrefix = "PagedTestDoc";

    for (int i = 1; i <= 10; i++) {
      Map<String, String> mapTuple = Maps.newHashMap();
      mapTuple.put("_id", testDocumentIdPrefix + i);
      mapTuple.put("name", "PTD" + i);
      mapTuple.put("type", "test");
      CouchDBTestHelper.get().insertDocument(mapTuple);
    }

    TestPagedDBInputOperator operatorTest = new TestPagedDBInputOperator();
    CollectorTestSink sink = new CollectorTestSink();
    operatorTest.outputPort.setSink(sink);
    operatorTest.setPageSize(5);
    operatorTest.setDatabase(CouchDBTestHelper.get().getDatabase());
    operatorTest.setup(new OperatorContextTestHelper.TestIdOperatorContext(3));

    int totalDocsInDb = CouchDBTestHelper.get().getTotalDocuments();
    int rounds = (totalDocsInDb % 5 == 0 ? 0 : 1) + (totalDocsInDb / 5);

    int remainingDocCount = totalDocsInDb;
    for (int i = 0; i < rounds; i++) {
      operatorTest.beginWindow(i);
      operatorTest.emitTuples();
      operatorTest.endWindow();
      Assert.assertEquals("number emitted tuples", remainingDocCount > 5 ? 5 : remainingDocCount, sink.collectedTuples.size());
      remainingDocCount = remainingDocCount - 5;
      sink.clear();
    }
  }
}
