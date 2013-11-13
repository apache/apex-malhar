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

import com.datatorrent.api.Context;
import com.datatorrent.api.annotation.ShipContainingJars;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import javax.annotation.Nonnull;

/**
 * Base Couch-Db output operator that saves tuples in the couchdb.<br></br>
 * The tuples need to be converted to {@link CouchDbUpdateCommand}.
 * This conversion is done by subclasses. <br></br>
 *
 * @param <T> type of tuple </T>
 * @since 0.3.5
 */
@ShipContainingJars(classes = {JsonNode.class, ObjectMapper.class, ObjectNode.class, JsonProperty.class})
public abstract class AbstractCouchDBOutputOperator<T> extends AbstractDBOutputOperator<T> implements CouchDbOperator
{
  private final static String LAST_WINDOW_FIELD = "lastWindow";

  private transient CouchDBLink dbLink;
  private transient ObjectMapper mapper;

  private String url;
  private String userName;
  private String password;

  @Nonnull
  private String dbName;

  /**
   * when this flag is set the operator will ignore if the document that needs to be
   * saved in the database is missing revision field. In that case it retrieves the
   * last revision of that document.
   */
  private boolean updateRevisionWhenNull = false;

  @Override
  public void setUrl(String url)
  {
    this.url = url;
  }

  @Override
  public void setDatabase(String dbName)
  {
    this.dbName = dbName;
  }

  @Override
  public void setUserName(String userName)
  {
    this.userName = userName;
  }

  @Override
  public void setPassword(String password)
  {
    this.password = password;
  }

  /**
   * Sets the flag which controls whether or not to accept documents which have revision null.
   *
   * @param updateRevisionWhenNull when set true, the operator will allow documents which don't have the revision field.
   */
  public void setUpdateRevisionWhenNull(boolean updateRevisionWhenNull)
  {
    this.updateRevisionWhenNull = updateRevisionWhenNull;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    this.dbLink = new CouchDBLink(url, userName, password, dbName);
    this.mapper = new ObjectMapper();
    super.setup(context);
  }

  @Override
  public Long getLastPersistedWindow()
  {
    JsonNode rootNode = getLastWindowDoc();
    JsonNode lastWindowField = rootNode.get(LAST_WINDOW_FIELD);
    if (lastWindowField != null) {
      return lastWindowField.getLongValue();
    }
    return null;
  }

  @Override
  public void storeData(T tuple)
  {
    CouchDbUpdateCommand couchDbUpdateCommand = getCommandToUpdateDb(tuple);
    String docId = couchDbUpdateCommand.getId();
    if (docId != null && dbLink.getConnector().contains(docId)) {
      JsonNode docInDatabase = dbLink.getConnector().get(JsonNode.class, docId);

      if (docInDatabase != null && couchDbUpdateCommand.getRevision() == null && updateRevisionWhenNull)
        couchDbUpdateCommand.setRevision(docInDatabase.get("_rev").getTextValue());
      dbLink.getConnector().update(couchDbUpdateCommand.getPayLoad());
    }
    else {  //create a document & if docId is null then couch db will generate a random id.
      dbLink.getConnector().create(couchDbUpdateCommand.getPayLoad());
    }
  }

  @Override
  public void storeWindow(long windowId)
  {
    JsonNode rootNode = getLastWindowDoc();
    ((ObjectNode) rootNode).put(LAST_WINDOW_FIELD, windowId);
  }

  private String getWindowDocumentId()
  {
    return applicationName + "_" + applicationId + "_" + operatorId;
  }

  private JsonNode getLastWindowDoc()
  {
    String windowDoc = getWindowDocumentId();
    if (dbLink.getConnector().contains(windowDoc)) {
      return dbLink.getConnector().get(JsonNode.class, windowDoc);
    }
    else {
      JsonNode rootNode = mapper.createObjectNode();
      dbLink.getConnector().create(windowDoc, rootNode);
      return rootNode;
    }
  }

  /**
   * Concrete implementation of this operator should provide the command to insert the tuple of type T in CouchDB.
   */
  public abstract CouchDbUpdateCommand getCommandToUpdateDb(T tuple);
}
