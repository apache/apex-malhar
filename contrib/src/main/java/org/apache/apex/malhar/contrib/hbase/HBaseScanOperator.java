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

import java.io.IOException;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import com.google.common.collect.Queues;
import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator;

/**
 * A base implementation of hbase input operator that retrieves tuples from
 * HBase columns and provides scan operation. <br>
 * <p>
 * <br>
 * This class provides a HBase input operator that can be used to retrieve
 * tuples from rows in a HBase table. The class should be extended by the
 * end-operator developer. The extending class should implement operationScan
 * and getTuple methods. The operationScan method should provide a HBase Scan
 * metric object that specifies where to retrieve the tuple information from the
 * table. The getTuple method should map the contents of a Result from the Scan
 * result to a tuple.<br>
 *
 * <br>
 *
 * @displayName HBase Scan
 * @category Output
 * @tags hbase, scan, input operator
 * @since 0.3.2
 */
public abstract class HBaseScanOperator<T> extends HBaseInputOperator<T> implements Operator.ActivationListener<Context>
{
  public static final int DEF_QUEUE_SIZE = 1000;
  public static final int DEF_SLEEP_MILLIS = 10;

  private String startRow;
  private String endRow;
  private String lastReadRow;
  private int queueSize = DEF_QUEUE_SIZE;
  private int sleepMillis = DEF_SLEEP_MILLIS;
  private Queue<Result> resultQueue;
  private volatile boolean running;

  @AutoMetric
  protected long tuplesRead;

  // Transients
  protected transient Scan scan;
  protected transient ResultScanner scanner;
  protected transient Thread readThread;
  private transient String threadFailureReason = null;

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    resultQueue = Queues.newLinkedBlockingQueue(queueSize);
  }

  @Override
  public void activate(Context context)
  {
    startReadThread();
  }

  protected void startReadThread()
  {
    try {
      scan = operationScan();
      scanner = getStore().getTable().getScanner(scan);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    readThread = new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        try {
          Result result;
          while (true) {
            if ((result = scanner.next()) != null) {
              while ( running && !resultQueue.offer(result)) {
                Thread.sleep(sleepMillis);
              }
            }
          }
        } catch (Exception e) {
          logger.debug("Exception in fetching results {}", e.getMessage());
          threadFailureReason = e.getMessage();
        } finally {
          scanner.close();
        }
      }
    });
    readThread.start();
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    tuplesRead = 0;
    running = true;
  }

  @Override
  public void emitTuples()
  {
    if (!readThread.isAlive()) {
      throw new RuntimeException(threadFailureReason);
    }
    try {
      Result result = resultQueue.poll();
      if (result == null) {
        return;
      }
      T tuple = getTuple(result);
      if (tuple != null) {
        emitTuple(tuple);
        tuplesRead++;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void emitTuple(T tuple)
  {
    outputPort.emit(tuple);
  }

  @Override
  public void endWindow()
  {
    running = false;
    super.endWindow();
  }

  @Override
  public void deactivate()
  {
    readThread.interrupt();
  }

  /**
   * Return a HBase Scan metric to retrieve the tuple.
   * The implementor should return a HBase Scan metric that specifies where to retrieve the tuple from
   * the table.
   *
   * @return The HBase Get metric
   */
  protected abstract Scan operationScan();

   /**
   * Get a tuple from a HBase Scan result.
   * The implementor should map the contents of a Result from a Get result and return a tuple.
   *
   * @param result The result
   * @return The tuple
   */
  protected abstract T getTuple(Result result);

  /**
   * Returns the start row key in the table as set previously
   * @return {@link #startRow}
   */
  public String getStartRow()
  {
    return startRow;
  }

  /**
   * Sets the start row key in the table from where the scan should begin
   * @param startRow
   */
  public void setStartRow(String startRow)
  {
    this.startRow = startRow;
  }

  /**
   * Returns the end row key in the table as set previously
   * @return {@link #endRow}
   */
  public String getEndRow()
  {
    return endRow;
  }

  /**
   * Sets the end row key in the table where the scan should end
   * @param endRow
   */
  public void setEndRow(String endRow)
  {
    this.endRow = endRow;
  }

  /**
   * Returns the last read row key from the hbase table
   * @return {@link #lastReadRow}
   */
  public String getLastReadRow()
  {
    return lastReadRow;
  }

  /**
   * Sets the last read row key from the hbase table. After the failures, the new scan will start from this row key
   * @param lastReadRow
   */
  public void setLastReadRow(String lastReadRow)
  {
    this.lastReadRow = lastReadRow;
  }

  private static final Logger logger = LoggerFactory.getLogger(HBaseScanOperator.class);

}
