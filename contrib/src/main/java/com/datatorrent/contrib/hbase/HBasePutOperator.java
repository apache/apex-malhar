/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.hbase;

import java.io.IOException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

/**
  * Operator for storing tuples in HBase rows.<br>
 *
 *<br>
 * This class provides a HBase output operator that can be used to store tuples in rows in a
 * HBase table. It should be extended by the end-operator developer. The extending class should implement
 * operationPut method and provide a HBase Put operation object that specifies where and what to store for
 * the tuple in the table.<br>
 *
 * <br>
 * @param <T> The tuple type
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public abstract class HBasePutOperator<T> extends HBaseOutputOperator<T>
{

  @Override
  public void processTuple(T t) throws IOException {
      HTable table = getTable();
      Put put = operationPut(t);
      table.put(put);
  }

  public abstract Put operationPut(T t);
}
