package com.datatorrent.contrib.goldengate;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import javax.validation.constraints.NotNull;

import com.goldengate.atg.datasource.DsOperation;

import com.datatorrent.demos.goldengate.utils.*;

import com.datatorrent.api.DefaultOutputPort;

/**
 * Created by Pramod Immaneni <pramod@datatorrent.com> on 10/28/14.
 */
public class CSVTransactionInput extends AbstractDFSLineTailInput
{

  @NotNull
  private String tableName;

  public transient final DefaultOutputPort<_DsTransaction> outputPort = new DefaultOutputPort<_DsTransaction>();

  @Override
  protected void processLine(String line)
  {
    System.out.println(line);
    String[] columns = line.split(",");
    _TableName tableName = new _TableName();
    tableName.setFullName(this.tableName);
    _DsTransaction transaction = new _DsTransaction();
    transaction.setReadTime(Calendar.getInstance().getTime());
    _DsOperation operation = new _DsOperation();
    operation.setTableName(tableName);
    operation.setOperationType(DsOperation.OpType.DO_INSERT);
    operation.setPositionSeqno(5);
    operation.setNumCols(columns.length);
    List<_DsColumn> cols = new ArrayList<_DsColumn>();
    for (int i = 0; i < columns.length; ++i) {
      _DsColumn col = new _DsColumn();
      col.setAfterValue(columns[i]);
      cols.add(col);
    }
    operation.setCols(cols);
    List<_DsOperation> ops = new ArrayList<_DsOperation>();
    ops.add(operation);
    transaction.setOps(ops);
    outputPort.emit(transaction);
  }

  public String getTableName()
  {
    return tableName;
  }

  public void setTableName(String tableName)
  {
    this.tableName = tableName;
  }
}
