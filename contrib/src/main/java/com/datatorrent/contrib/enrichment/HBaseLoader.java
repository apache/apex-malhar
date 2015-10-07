package com.datatorrent.contrib.enrichment;

import com.datatorrent.contrib.hbase.HBaseStore;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * <p>HBaseLoader extends from {@link HBaseStore} uses HBase to connect and implements EnrichmentBackup interface.</p> <br/>
 *
 * Properties:<br>
 * <b>includeFamilys</b>: List of comma separated families and each family corresponds to the group name of column fields in includeFieldsStr. Ex: Family1,Family2<br>
 * <br>
 *
 * @displayName HBaseLoader
 * @tags Loader
 * @since 2.1.0
 */
public class HBaseLoader extends HBaseStore implements EnrichmentBackup
{
  protected transient List<String> includeFields;
  protected transient List<String> lookupFields;
  protected transient List<String> includeFamilys;

  protected Object getQueryResult(Object key)
  {
    try {
      Get get = new Get(getRowBytes(((ArrayList)key).get(0)));
      int idx = 0;
      for(String f : includeFields) {
        get.addColumn(Bytes.toBytes(includeFamilys.get(idx++)), Bytes.toBytes(f));
      }
      return getTable().get(get);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected ArrayList<Object> getDataFrmResult(Object result)
  {
    Result res = (Result)result;
    if (res == null || res.isEmpty())
      return null;
    ArrayList<Object> columnInfo = new ArrayList<Object>();

    if(CollectionUtils.isEmpty(includeFields)) {
      if(includeFields == null) {
        includeFields = new ArrayList<String>();
        includeFamilys.clear();
        includeFamilys = new ArrayList<String>();
      }
      for (KeyValue kv: res.raw()) {
        includeFields.add(new String(kv.getQualifier()));
        includeFamilys.add(new String(kv.getFamily()));
      }
    }
    for(KeyValue kv : res.raw()) {
      columnInfo.add(kv.getValue());
    }
    return columnInfo;
  }

  private byte[] getRowBytes(Object key)
  {
    return ((String)key).getBytes();
  }

  @Override public void setFields(List<String> lookupFields,List<String> includeFields)
  {
    this.includeFields = includeFields;
    this.lookupFields = lookupFields;
  }

  /**
   * Set the familyStr and would be in the form of comma separated list.
   */
  public void setIncludeFamilyStr(String familyStr)
  {
    this.includeFamilys = Arrays.asList(familyStr.split(","));
  }

  @Override public boolean needRefresh()
  {
    return false;
  }

  @Override public Map<Object, Object> loadInitialData()
  {
    return null;
  }

  @Override public Object get(Object key)
  {
    return getDataFrmResult(getQueryResult(key));
  }

  @Override public List<Object> getAll(List<Object> keys)
  {
    List<Object> values = Lists.newArrayList();
    for (Object key : keys) {
      values.add(get(key));
    }
    return values;
  }

  @Override public void put(Object key, Object value)
  {
    throw new RuntimeException("Not supported operation");
  }

  @Override public void putAll(Map<Object, Object> m)
  {
    throw new RuntimeException("Not supported operation");
  }

  @Override public void remove(Object key)
  {
    throw new RuntimeException("Not supported operation");
  }
}
