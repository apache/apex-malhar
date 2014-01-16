/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.redis;

import com.datatorrent.lib.db.AbstractKeyValueStoreInputOperator;

/**
 *
 * @param <T> The tuple type.
 * @since 0.9.3
 */
public abstract class AbstractRedisInputOperator<T> extends AbstractKeyValueStoreInputOperator<T, RedisStore>
{
}
