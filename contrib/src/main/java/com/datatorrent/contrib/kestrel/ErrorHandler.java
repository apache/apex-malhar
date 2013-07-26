/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.kestrel;

/**
 * <p>ErrorHandler interface.</p>
 *
 * @since 0.3.2
 */
public interface ErrorHandler {

    /**
     * Called for errors thrown during initialization.
     */
    public void handleErrorOnInit( final MemcachedClient client ,
                                   final Throwable error );

    /**
     * Called for errors thrown during {@link MemcachedClient#get(String)} and related methods.
     */
    public void handleErrorOnGet( final MemcachedClient client ,
                                  final Throwable error ,
                                  final String cacheKey );

    /**
     * Called for errors thrown during {@link MemcachedClient#getMulti(String)} and related methods.
     */
    public void handleErrorOnGet( final MemcachedClient client ,
                                  final Throwable error ,
                                  final String[] cacheKeys );

    /**
     * Called for errors thrown during {@link MemcachedClient#set(String,Object)} and related methods.
     */
    public void handleErrorOnSet( final MemcachedClient client ,
                                  final Throwable error ,
                                  final String cacheKey );

    /**
     * Called for errors thrown during {@link MemcachedClient#delete(String)} and related methods.
     */
    public void handleErrorOnDelete( final MemcachedClient client ,
                                     final Throwable error ,
                                     final String cacheKey );

    /**
     * Called for errors thrown during {@link MemcachedClient#flushAll()} and related methods.
     */
    public void handleErrorOnFlush( final MemcachedClient client ,
                                    final Throwable error );

    /**
     * Called for errors thrown during {@link MemcachedClient#stats()} and related methods.
     */
    public void handleErrorOnStats( final MemcachedClient client ,
                                    final Throwable error );

} // interface
