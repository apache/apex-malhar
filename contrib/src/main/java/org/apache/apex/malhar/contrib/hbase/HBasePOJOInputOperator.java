/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.contrib.hbase;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.util.FieldValueGenerator;
import org.apache.apex.malhar.lib.util.FieldValueGenerator.ValueConverter;
import org.apache.apex.malhar.lib.util.PojoUtils;
import org.apache.apex.malhar.lib.util.PojoUtils.Setter;
import org.apache.apex.malhar.lib.util.TableInfo;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;

/**
 * HBasePOJOInputOperator reads data from a HBase store, converts it to a POJO and puts it on the output port.
 * The read from HBase is asynchronous.
 * @displayName HBase Input Operator
 * @category Input
 * @tags database, nosql, pojo, hbase
 * @since 3.1.0
 */
@Evolving
public class HBasePOJOInputOperator extends HBaseScanOperator<Object>
{
  private TableInfo<HBaseFieldInfo> tableInfo;
  private String pojoTypeName;

  // Transients
  protected transient Class<?> pojoType;
  protected transient FieldValueGenerator<HBaseFieldInfo> fieldValueGenerator;
  protected transient BytesValueConverter valueConverter;
  private transient Scan scan;
  private transient Setter<Object, String> rowSetter;

  public static class BytesValueConverter implements ValueConverter<HBaseFieldInfo>
  {
    @Override
    public Object convertValue( HBaseFieldInfo fieldInfo, Object value)
    {
      return fieldInfo.toValue( (byte[])value );
    }
  }

  public final transient DefaultOutputPort<Object> outputPort = new DefaultOutputPort<Object>()
  {
    public void setup(com.datatorrent.api.Context.PortContext context)
    {
      pojoType = context.getAttributes().get(Context.PortContext.TUPLE_CLASS);
    }
  };

  @Override
  public void activate(Context context)
  {
    try {
      pojoType.newInstance(); // try create new instance to verify the class.
      rowSetter = PojoUtils.createSetter(pojoType, tableInfo.getRowOrIdExpression(), String.class);
      fieldValueGenerator = new HBaseFieldValueGenerator(pojoType, tableInfo.getFieldsInfo());
      valueConverter = new BytesValueConverter();
      scan = new Scan();
      super.activate(context);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  protected Object getTuple(Result result)
  {
    try {
      String readRow = Bytes.toString(result.getRow());
      if ( readRow.equals( getLastReadRow() )) {
        return null;
      }

      Object instance = pojoType.newInstance();
      rowSetter.set(instance, readRow);

      List<Cell> cells = result.listCells();
      for (Cell cell : cells) {
        String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
        String columnFamily = Bytes.toString(CellUtil.cloneFamily(cell));
        byte[] value = CellUtil.cloneValue(cell);
        ((HBaseFieldValueGenerator)fieldValueGenerator).setColumnValue(instance, columnName, columnFamily, value,
            valueConverter);
      }

      setLastReadRow(readRow);
      return instance;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected Scan operationScan()
  {
    if (getLastReadRow() == null && getStartRow() == null) {
      // If no start row specified and no row read yet
      if (scan == null) {
        scan = new Scan();
      }
    } else if (getEndRow() == null) {
      // If only start row specified
      scan.setStartRow(Bytes.toBytes(getLastReadRow() == null ? getStartRow() : getLastReadRow()));
    } else {
      // If end row also specified
      scan.setStartRow(Bytes.toBytes(getLastReadRow() == null ? getStartRow() : getLastReadRow()));
      scan.setStopRow(Bytes.toBytes(getEndRow()));
    }
    for (HBaseFieldInfo field : tableInfo.getFieldsInfo()) {
      scan.addColumn(Bytes.toBytes(field.getFamilyName()), Bytes.toBytes(field.getColumnName()));
    }
    return scan;
  }

  @Override
  protected void emitTuple(Object tuple)
  {
    outputPort.emit(tuple);
  }

  /**
   * Returns the {@link #tableInfo} object as configured
   * @return {@link #tableInfo}
   */
  public TableInfo<HBaseFieldInfo> getTableInfo()
  {
    return tableInfo;
  }

  /**
   * Sets the {@link #tableInfo} object
   * @param tableInfo
   */
  public void setTableInfo(TableInfo<HBaseFieldInfo> tableInfo)
  {
    this.tableInfo = tableInfo;
  }

  /**
   * Returns the POJO class name
   * @return {@link #pojoTypeName}
   */
  public String getPojoTypeName()
  {
    return pojoTypeName;
  }

  /**
   * Sets the POJO class name
   * @param pojoTypeName
   */
  public void setPojoTypeName(String pojoTypeName)
  {
    this.pojoTypeName = pojoTypeName;
  }

  private static final Logger logger = LoggerFactory.getLogger(HBasePOJOInputOperator.class);

}
