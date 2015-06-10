package com.datatorrent.contrib.hbase;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.datatorrent.contrib.common.FieldValueGenerator;
import com.datatorrent.contrib.common.TableInfo;
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
  private static final long serialVersionUID = 3241368443399294019L;

  private TableInfo<HBaseFieldInfo> tableInfo;

  private transient FieldValueGenerator<HBaseFieldInfo> fieldValueGenerator;

  private transient Getter<Object, String> rowGetter;

  @Override
  public Put operationPut(Object obj)
  {
    final List<HBaseFieldInfo> fieldsInfo = tableInfo.getFieldsInfo();
    if (fieldValueGenerator == null) {
      fieldValueGenerator = FieldValueGenerator.getFieldValueGenerator(obj.getClass(), fieldsInfo);
    }
    if (rowGetter == null) {
      // use string as row id
      rowGetter = PojoUtils.createGetter(obj.getClass(), tableInfo.getRowOrIdExpression(), String.class);
    }

    Map<HBaseFieldInfo, Object> valueMap = fieldValueGenerator.getFieldsValue(obj);
    Put put = new Put(Bytes.toBytes(rowGetter.get(obj)));
    for (Map.Entry<HBaseFieldInfo, Object> entry : valueMap.entrySet()) {
      HBaseFieldInfo fieldInfo = entry.getKey();
      put.add(Bytes.toBytes(fieldInfo.getFamilyName()), Bytes.toBytes(fieldInfo.getColumnName()), fieldInfo.toBytes(entry.getValue()));
    }
    return put;
  }

  /**
   * HBase table information
   */
  public TableInfo<HBaseFieldInfo> getTableInfo()
  {
    return tableInfo;
  }

  /**
   * HBase table information
   */
  public void setTableInfo(TableInfo<HBaseFieldInfo> tableInfo)
  {
    this.tableInfo = tableInfo;
  }

}
