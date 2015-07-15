package com.datatorrent.contrib.enrichment;

import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.Test;

public class MapEnrichmentOperatorTest
{
  @Test
  public void includeAllKeys()
  {
    MapEnrichmentOperator oper = new MapEnrichmentOperator();
    oper.setStore(new MemoryStore());
    oper.setLookupFieldsStr("In1");
    oper.setup(null);

    CollectorTestSink sink = new CollectorTestSink();
    TestUtils.setSink(oper.output, sink);

    Map<String, Object> inMap = Maps.newHashMap();
    inMap.put("In1", "Value1");
    inMap.put("In2", "Value2");

    oper.beginWindow(1);
    oper.input.process(inMap);
    oper.endWindow();

    Assert.assertEquals("includeSelectedKeys: Number of tuples emitted: ", 1, sink.collectedTuples.size());
    Assert.assertEquals("Enrich Tuple: ", "{A=Val_A, B=Val_B, C=Val_C, In2=Value2, In1=Value3}", sink.collectedTuples.get(0).toString());
  }

  @Test
  public void includeSelectedKeys()
  {
    MapEnrichmentOperator oper = new MapEnrichmentOperator();
    oper.setStore(new MemoryStore());
    oper.setLookupFieldsStr("In1");
    oper.setIncludeFieldsStr("A,B");
    oper.setup(null);

    CollectorTestSink sink = new CollectorTestSink();
    TestUtils.setSink(oper.output, sink);

    Map<String, Object> inMap = Maps.newHashMap();
    inMap.put("In1", "Value1");
    inMap.put("In2", "Value2");

    oper.beginWindow(1);
    oper.input.process(inMap);
    oper.endWindow();

    Assert.assertEquals("includeSelectedKeys: Number of tuples emitted: ", 1, sink.collectedTuples.size());
    Assert.assertEquals("Enrich Tuple: ", "{A=Val_A, B=Val_B, In2=Value2, In1=Value1}", sink.collectedTuples.get(0).toString());
  }

  private static class MemoryStore implements EnrichmentBackup
  {
    static Map<String, Map> returnData = Maps.newHashMap();
    private List<String> includeFields;
    static {
      Map<String, String> map = Maps.newHashMap();
      map.put("A", "Val_A");
      map.put("B", "Val_B");
      map.put("C", "Val_C");
      map.put("In1", "Value3");
      returnData.put("Value1", map);

      map = Maps.newHashMap();
      map.put("A", "Val_A_1");
      map.put("B", "Val_B_1");
      map.put("C", "Val_C");
      map.put("In1", "Value3");
      returnData.put("Value2", map);
    }

    @Override public Map<Object, Object> loadInitialData()
    {
      return null;
    }

    @Override public Object get(Object key)
    {
      List<String> keyList = (List<String>)key;
      Map<String, String> keyValue = returnData.get(keyList.get(0));
      ArrayList<Object> lst = new ArrayList<Object>();
      if(CollectionUtils.isEmpty(includeFields)) {
        if(includeFields == null)
          includeFields = new ArrayList<String>();
        for (Map.Entry<String, String> e : keyValue.entrySet()) {
          includeFields.add(e.getKey());
        }
      }
      for(String field : includeFields) {
        lst.add(keyValue.get(field));
      }
      return lst;
    }

    @Override public List<Object> getAll(List<Object> keys)
    {
      return null;
    }

    @Override public void put(Object key, Object value)
    {

    }

    @Override public void putAll(Map<Object, Object> m)
    {

    }

    @Override public void remove(Object key)
    {

    }

    @Override public void connect() throws IOException
    {

    }

    @Override public void disconnect() throws IOException
    {

    }

    @Override public boolean isConnected()
    {
      return false;
    }

    @Override public void setFields(List<String> lookupFields, List<String> includeFields)
    {
      this.includeFields = includeFields;

    }

    @Override
    public boolean needRefresh() {
      return false;
    }
  }
}
