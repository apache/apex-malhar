package com.datatorrent.demos.goldengate;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.contrib.goldengate.lib.AbstractDFSLineTailInput;
import com.datatorrent.demos.goldengate.utils._DsColumn;
import com.datatorrent.demos.goldengate.utils._DsOperation;
import com.datatorrent.demos.goldengate.utils._DsTransaction;
import com.datatorrent.demos.goldengate.utils._TableName;
import com.goldengate.atg.datasource.DsOperation;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import javax.validation.constraints.NotNull;

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
