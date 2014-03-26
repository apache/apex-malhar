/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.db;

/**
 *
 * @param <T>
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public interface TransactionalDataStoreWriter<T> extends DataStoreWriter<T>, TransactionableStore
{
}
