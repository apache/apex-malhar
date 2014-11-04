/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.db.jdbc;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link JdbcNonTransactionalStore}
 */
public class JdbcNonTransactionalStoreTest
{
  @Test
  public void beginTransactionTest()
  {
    JdbcNonTransactionalStore jdbcNonTransactionalStore = new JdbcNonTransactionalStore();
    boolean threwException = false;
    try {
      jdbcNonTransactionalStore.beginTransaction();
    }
    catch(RuntimeException e) {
      threwException = true;
    }
    Assert.assertTrue("Exception should be thrown", threwException);
  }

  @Test
  public void commitTransactionTest()
  {
    JdbcNonTransactionalStore jdbcNonTransactionalStore = new JdbcNonTransactionalStore();
    boolean threwException = false;
    try {
      jdbcNonTransactionalStore.commitTransaction();
    }
    catch(RuntimeException e) {
      threwException = true;
    }
    Assert.assertTrue("Exception should be thrown", threwException);
  }

  @Test
  public void rollbackTransactionTest()
  {
    JdbcNonTransactionalStore jdbcNonTransactionalStore = new JdbcNonTransactionalStore();
    boolean threwException = false;
    try {
      jdbcNonTransactionalStore.rollbackTransaction();
    }
    catch(RuntimeException e) {
      threwException = true;
    }
    Assert.assertTrue("Exception should be thrown", threwException);
  }

  @Test
  public void isInTransactionTest()
  {
    JdbcNonTransactionalStore jdbcNonTransactionalStore = new JdbcNonTransactionalStore();
    boolean threwException = false;
    try {
      jdbcNonTransactionalStore.isInTransaction();
    }
    catch(RuntimeException e) {
      threwException = true;
    }
    Assert.assertTrue("Exception should be thrown", threwException);
  }
}
