package com.datatorrent.demos.goldengate.utils;

import java.io.PrintWriter;

import com.goldengate.atg.datasource.DsColumn;
import com.goldengate.atg.datasource.DsOperation;
import com.goldengate.atg.datasource.DsTransaction;
import com.goldengate.atg.datasource.format.DsFormatterAdapter;
import com.goldengate.atg.datasource.meta.ColumnMetaData;
import com.goldengate.atg.datasource.meta.DsMetaData;
import com.goldengate.atg.datasource.meta.TableMetaData;
import com.goldengate.atg.datasource.meta.TableName;

public class DefaultFormatter extends DsFormatterAdapter
{

  @Override
  public void formatOp(DsTransaction arg0, DsOperation arg1, TableMetaData arg2, PrintWriter arg3)
  {

  }

  @Override
  public void formatTx(DsTransaction tx, DsMetaData meta, PrintWriter out)
  {
    out.print("Transaction: ");
    out.print("numOps=\'" + tx.getSize() + "\' ");
    out.println("ts=\'" + tx.getStartTxTimeAsString() + "\'");
    for (DsOperation op : tx.getOperations()) {
      TableName currTable = op.getTableName();
      TableMetaData tMeta = meta.getTableMetaData(currTable);
      String opType = op.getOperationType().toString();
      String table = tMeta.getTableName().getFullName();
      out.println(opType + " on table \"" + table + "\":");
      int colNum = 0;
      for (DsColumn col : op.getColumns()) {
        ColumnMetaData cMeta = tMeta.getColumnMetaData(colNum++);
        out.println(cMeta.getColumnName() + " = " + col.getAfterValue());
      }
    }

  }

}
