/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.hbase;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.HTable;

/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public abstract class HBaseAppendOperator<T> extends HBaseOutputOperator<T>
{

  @Override
  public void processTuple(T t) throws IOException {
      HTable table = getTable();
      Append append = operationAppend(t);
      table.append(append);
  }

  public abstract Append operationAppend(T t);
}
