package com.datatorrent.contrib.couchdb;

import com.datatorrent.api.Context;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import javax.annotation.Nonnull;

/**
 * Base Couch-Db output operator that saves tuples in the couchdb.<br></br>
 * The tuples need to be converted to {@link CouchDbTuple}. This conversion is done by subclasses. <br></br>
 *
 * @param <T> type of tuple </T>
 * @since 0.3.5
 */
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
    CouchDbTuple couchDbTuple = getCouchDbTuple(tuple);
    String docId = couchDbTuple.getId();
    if (docId != null && dbLink.getConnector().contains(docId)) {
      JsonNode docNode = dbLink.getConnector().get(JsonNode.class, docId);
      if (docNode != null && couchDbTuple.getRevision() == null && updateRevisionWhenNull)
        couchDbTuple.setRevision(docNode.get("_rev").getTextValue());
    }
    dbLink.getConnector().update(couchDbTuple.getPayLoad());
  }

  @Override
  public void storeWindow(long windowId)
  {
    JsonNode rootNode = getLastWindowDoc();
    ((ObjectNode) rootNode).put(LAST_WINDOW_FIELD, windowId);
  }

  @Override
  public String getDefaultApplicationName()
  {
    return "CouchDbOutput";
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
    } else {
      JsonNode rootNode = mapper.createObjectNode();
      dbLink.getConnector().create(windowDoc, rootNode);
      return rootNode;
    }
  }

  public abstract CouchDbTuple getCouchDbTuple(T tuple);
}
