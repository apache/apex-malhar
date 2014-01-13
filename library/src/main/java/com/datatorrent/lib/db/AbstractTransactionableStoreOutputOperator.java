/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.db;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import java.io.IOException;
import javax.validation.constraints.NotNull;

/**
 *
 * @since 0.9.3
 */
public abstract class AbstractTransactionableStoreOutputOperator<T> extends BaseOperator
{
  protected TransactionableStore store;
  protected boolean passThru = false;
  protected transient boolean inTransaction = false;
  @NotNull
  protected String appId;
  @NotNull
  protected Integer operatorId;
  protected long currentWindowId = -1;
  protected long committedWindowId = -1;

  @InputPortFieldAnnotation(name = "in", optional = true)
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T t)
    {
      if (committedWindowId < currentWindowId) {
        processTuple(t);
      }
    }

  };

  public void setAppId(String appId)
  {
    this.appId = appId;
  }

  public void setOperatorId(int operatorId)
  {
    this.operatorId = operatorId;
  }

  public void setStore(TransactionableStore store)
  {
    this.store = store;
  }

  public boolean isInTransaction()
  {
    return inTransaction;
  }

  public void setInTransaction(boolean inTransaction)
  {
    this.inTransaction = inTransaction;
  }

  public boolean isPassThru()
  {
    return passThru;
  }

  public void setPassThru(boolean passThru)
  {
    this.passThru = passThru;
  }

  @Override
  public void setup(OperatorContext context)
  {
    try {
      appId = context.getValue(DAG.APPLICATION_ID);
      operatorId = context.getId();

      store.connect();
      committedWindowId = getCommittedWindowId(appId, operatorId);
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;
  }

  protected abstract long getCommittedWindowId(String appId, int operatorId);

  protected abstract void storeCommittedWindowId(String appId, int operatorId, long windowId);

  @Override
  public void teardown()
  {
    try {
      if (inTransaction) {
        store.rollbackTransaction();
      }
      store.disconnect();
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   *
   * @param tuple
   */
  public abstract void processTuple(T tuple);

}
