package com.datatorrent.contrib.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.contrib.util.FieldValueGenerator;
import com.datatorrent.contrib.util.FieldValueGenerator.ValueConverter;
import com.datatorrent.contrib.util.TableInfo;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;
import com.datatorrent.lib.util.PojoUtils.Setter;

public class HBasePOJOInputOperator extends HBaseInputOperator<Object>
{
  private TableInfo<HBaseFieldInfo> tableInfo;
  protected HBaseStore store;
  private String pojoTypeName;
  private String startRow;
  private String lastReadRow;
  
  protected transient Class pojoType;
  private transient Setter<Object, String> rowSetter;
  protected transient FieldValueGenerator<HBaseFieldInfo> fieldValueGenerator;
  protected transient BytesValueConverter valueConverter;
  
  public static class BytesValueConverter implements ValueConverter<HBaseFieldInfo>
  {
    @Override
    public Object convertValue( HBaseFieldInfo fieldInfo, Object value)
    {
      return fieldInfo.toValue( (byte[])value );
    }
  }
  
  @Override
  public void setup(OperatorContext context)
  {
    try {
      store.connect();
      pojoType = Class.forName(pojoTypeName);
      pojoType.newInstance();   //try create new instance to verify the class.
      rowSetter = PojoUtils.createSetter(pojoType, tableInfo.getRowOrIdExpression(), String.class);
      fieldValueGenerator = FieldValueGenerator.getFieldValueGenerator(pojoType, tableInfo.getFieldsInfo() );
      valueConverter = new BytesValueConverter();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void teardown()
  {
    try {
      store.disconnect();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void emitTuples()
  {
    try {
      Scan scan = nextScan();
      if (scan == null)
        return;

      ResultScanner resultScanner = store.getTable().getScanner(scan);

      while (true) {
        Result result = resultScanner.next();
        if (result == null)
          break;

        String readRow = Bytes.toString(result.getRow());
        if( readRow.equals( lastReadRow ))
          continue;

        Object instance = pojoType.newInstance();
        rowSetter.set(instance, readRow);
        
        List<Cell> cells = result.listCells();

        for (Cell cell : cells) {
          String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
          byte[] value = CellUtil.cloneValue(cell);
          fieldValueGenerator.setColumnValue( instance, columnName, value, valueConverter );
        }
        
        outputPort.emit(instance);
        lastReadRow = readRow;
      }

    } catch (Exception e) {
      throw new RuntimeException(e.getMessage());
    }

  }
  
  protected Scan nextScan()
  {
    if(lastReadRow==null && startRow==null )
      return new Scan();
    else
      return new Scan( Bytes.toBytes( lastReadRow == null ? startRow : lastReadRow ) );
  }

  public HBaseStore getStore()
  {
    return store;
  }
  public void setStore(HBaseStore store)
  {
    this.store = store;
  }

  public TableInfo<HBaseFieldInfo> getTableInfo()
  {
    return tableInfo;
  }

  public void setTableInfo(TableInfo<HBaseFieldInfo> tableInfo)
  {
    this.tableInfo = tableInfo;
  }

  public String getPojoTypeName()
  {
    return pojoTypeName;
  }

  public void setPojoTypeName(String pojoTypeName)
  {
    this.pojoTypeName = pojoTypeName;
  }
  
  

}
