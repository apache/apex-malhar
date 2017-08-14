/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.contrib.couchdb;

import java.io.IOException;
import java.net.MalformedURLException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import org.ektorp.CouchDbConnector;
import org.ektorp.ViewQuery;
import org.ektorp.ViewResult;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;

import org.apache.apex.malhar.lib.db.Connectable;

/**
 * Implements a CouchDb store. <br/>
 * <p>
 * Operates in At-most once recovery mode.
 * @displayName CouchDb Store
 * @category Output
 * @tags couchdb
 * @since 0.3.5
 */
public class CouchDbStore implements Connectable
{
  /**
   * default value: http://localhost:5984
   */
  private String dbUrl;
  private String userName;
  private String password;
  @NotNull
  private String dbName;

  private transient CouchDbConnector dbConnector;
  private transient StdCouchDbInstance couchInstance;

  /**
   * Sets the database URL.
   *
   * @param dbUrl database url.
   */
  public void setDbUrl(String dbUrl)
  {
    this.dbUrl = dbUrl;
  }

  /**
   * Sets the database user.
   *
   * @param userName user name.
   */
  public void setUserName(String userName)
  {
    this.userName = userName;
  }

  /**
   * Sets the password of database user.
   *
   * @param password password of the database user.
   */
  public void setPassword(String password)
  {
    this.password = password;
  }

  /**
   * Sets the database.
   *
   * @param dbName name of the database.
   */
  public void setDbName(@Nonnull String dbName)
  {
    this.dbName = dbName;
  }

  /**
   * Returns if a document identified by the document id is present in the database or not.
   *
   * @param docId document id.
   * @return true if the document is in the database; false otherwise.
   */
  public boolean containsDocument(String docId)
  {
    return dbConnector.contains(docId);
  }

  /**
   * Inserts a document in the store.
   *
   * @param docId    document id.
   * @param document document in the form of JsonNode.
   */
  public void insertDocument(String docId, @Nonnull Object document)
  {
    dbConnector.create(docId, document);
  }

  /**
   * Returns a document identified by the docId from the database.
   *
   * @param docId document id.
   * @return document in the database in JsonNode format.
   */
  @Nullable
  public <T> T getDocument(String docId, Class<T> docType)
  {
    return dbConnector.get(docType, docId);
  }

  /**
   * Update or insert a document identified by docId in the database.
   *
   * @param docId    document id.
   * @param document document.
   */
  public void upsertDocument(String docId, @Nonnull Object document)
  {
    if (docId != null && dbConnector.contains(docId)) {
      dbConnector.update(document);
    } else {
      //create a document & if docId is null then couch db will generate a random id.
      dbConnector.create(document);
    }
  }

  /**
   * Returns the results of a view.
   *
   * @param viewQuery view query that represents a view in couch-db.
   * @return result of view.
   */
  public ViewResult queryStore(ViewQuery viewQuery)
  {
    return dbConnector.queryView(viewQuery);
  }

  @Override
  public void connect() throws IOException
  {
    StdHttpClient.Builder builder = new StdHttpClient.Builder();
    if (dbUrl != null) {
      try {
        builder.url(dbUrl);
      } catch (MalformedURLException e) {
        throw new IllegalArgumentException(e.getMessage());
      }
    }
    if (userName != null) {
      builder.username(userName);
    }
    if (password != null) {
      builder.password(password);
    }

    HttpClient httpClient = builder.build();
    couchInstance = new StdCouchDbInstance(httpClient);
    dbConnector = couchInstance.createConnector(dbName, false);
  }

  @Override
  public void disconnect() throws IOException
  {
    couchInstance.getConnection().shutdown();
    dbConnector = null;
  }

  @Override
  public boolean isConnected()
  {
    return dbConnector == null;
  }
}
