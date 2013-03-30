/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.hbase;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Append;

/**
 * Operator for storing tuples in HBase columns.<br>
 *
 *<br>
 * This class provides a HBase output operator that can be used to store tuples in columns in a
 * HBase table. It should be extended by the end-operator developer. The extending class should implement
 * operationAppend method and provide a HBase Append operation object that specifies where and what to
 * store for the tuple in the table.<br>
 *
 * <br>
 * @param <T> The tuple type
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public abstract class HBaseAppendOperator<T> extends HBaseOutputOperator<T>
{

  /**
   * Process the tuple.
   * Store the tuple in HBase. The method gets a HBase Append operation from the concrete implementation
   * and uses it to store the tuple.
   *
   * @param t The tuple
   * @throws IOException
   */
  @Override
  public void processTuple(T t) throws IOException {
      Append append = operationAppend(t);
      table.append(append);
  }

  /**
   * Return the HBase Append operation to store the tuple.
   * The implementor should return a HBase Append operation that specifies where and what to store for the tuple
   * in the table.
   *
   * @param t The tuple
   * @return The HBase Append operation
   */
  public abstract Append operationAppend(T t);
}
