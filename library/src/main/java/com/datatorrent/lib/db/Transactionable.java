/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.db;

/**
 *
 * @since 0.9.3
 */
public interface Transactionable
{
  public void beginTransaction();
  public void commitTransaction();
  public void rollbackTransaction();
}
