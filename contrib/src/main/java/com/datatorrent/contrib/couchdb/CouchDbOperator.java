/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.couchdb;

/**
 * Interface that is implemented by CounchDb Input and Output adaptors.</br>
 *
 * @since 0.3.5
 */
public interface CouchDbOperator
{
  /**
   * Sets the database connection url
   * @param url database connection url
   */
  void setUrl(String url);

  /**
   * Sets the database name
   * @param dbName  database name
   */
  void setDatabase(String dbName);

  /**
   * Sets the database user
   * @param userName database user
   */
  void setUserName(String userName);

  /**
   * Sets the password of the database user
   * @param password password of the database user
   */
  void setPassword(String password);
}
