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

package com.datatorrent.contrib.memsql;

import com.datatorrent.lib.db.jdbc.JdbcNonTransactionalStore;

/**
 * A connection store for memsql which has the default connection driver set
 * to be the mysql connection driver.
 *
 * @since 2.0.0
 */
public class MemsqlStore extends JdbcNonTransactionalStore
{
  public static final String MEMSQL_DRIVER = "com.mysql.jdbc.Driver";

  public MemsqlStore()
  {
    super();
    this.setDbDriver(MEMSQL_DRIVER);
  }
}
