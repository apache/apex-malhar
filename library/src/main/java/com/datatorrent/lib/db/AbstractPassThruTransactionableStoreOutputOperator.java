/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.db;

/**
 *
 * @since 0.9.3
 */
public abstract class AbstractPassThruTransactionableStoreOutputOperator<T, S extends TransactionableStore> extends AbstractTransactionableStoreOutputOperator<T, S>
{

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    store.beginTransaction();
    setInTransaction(true);
  }

  @Override
  public void endWindow()
  {
    storeCommittedWindowId(appId, operatorId, currentWindowId);
    store.commitTransaction();
    setInTransaction(false);
    committedWindowId = currentWindowId;
    super.endWindow();
  }

}
