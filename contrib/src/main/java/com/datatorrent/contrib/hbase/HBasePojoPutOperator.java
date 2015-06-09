package com.datatorrent.contrib.hbase;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.datatorrent.contrib.common.FieldValueGenertor;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;

/**
 * 
 * @displayName HBase Put Output
 * @category Database
 * @tags hbase put, output operator, Pojo
 */
public class HBasePojoPutOperator extends AbstractHBasePutOutputOperator<Object>
{
  private HBaseTableInfo tableInfo;

  private transient FieldValueGenertor<HBaseFieldInfo> fieldValueGenerator;

  private transient Getter<Object, String> rowGetter;

  @Override
  public Put operationPut(Object obj)
  {
    final List<HBaseFieldInfo> fieldsInfo = tableInfo.getFieldsInfo();
    if (fieldValueGenerator == null)
    {
      fieldValueGenerator = FieldValueGenertor.getFieldValueGenerator(obj.getClass(), fieldsInfo);
    }
    if (rowGetter == null)
    {
      // use string as row id
      rowGetter = PojoUtils.createGetter(obj.getClass(), tableInfo.getRowOrIdExpression(), String.class);
    }

    Map<HBaseFieldInfo, Object> valueMap = fieldValueGenerator.getFieldsValue(obj);
    Put put = new Put(Bytes.toBytes(rowGetter.get(obj)));
    for (Map.Entry<HBaseFieldInfo, Object> entry : valueMap.entrySet())
    {
      HBaseFieldInfo fieldInfo = entry.getKey();
      put.add(Bytes.toBytes(fieldInfo.getFamilyName()), Bytes.toBytes(fieldInfo.getColumnName()),
          fieldInfo.toBytes(entry.getValue()));
    }
    return put;
  }

  /**
   * HBase table information
   */
  public HBaseTableInfo getTableInfo()
  {
    return tableInfo;
  }

  /**
   * HBase table information
   */
  public void setTableInfo(HBaseTableInfo tableInfo)
  {
    this.tableInfo = tableInfo;
  }

}
