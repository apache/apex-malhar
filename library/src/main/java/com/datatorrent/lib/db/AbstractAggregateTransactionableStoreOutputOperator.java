/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.db;

/**
 *
 * @since 0.9.3
 */
public abstract class AbstractAggregateTransactionableStoreOutputOperator<T> extends AbstractTransactionableStoreOutputOperator<T>
{
  @Override
  public void endWindow()
  {
    store.beginTransaction();
    setInTransaction(true);
    storeAggregate();
    storeCommittedWindowId(appId, operatorId, currentWindowId);
    store.commitTransaction();
    setInTransaction(false);
    committedWindowId = currentWindowId;
    super.endWindow();
  }

  /**
   * Stores the aggregated state to persistent store at end window.
   *
   */
  public abstract void storeAggregate();
}
