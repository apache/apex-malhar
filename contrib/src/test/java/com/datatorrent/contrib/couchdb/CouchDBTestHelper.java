package com.datatorrent.contrib.couchdb;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.ektorp.ViewQuery;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * <br>A helper class that setups the couch db for testing</br>
 *
 * @since 0.3.5
 */
public class CouchDBTestHelper
{
  private static CouchDBTestHelper helper;
  private static final String TEST_DB = "CouchDbTest";
  private static final String DESIGN_DOC_ID = "_design/CouchDbTest";
  private static final String TEST_VIEW = "testView";

  @Nullable
  private final String dbUrl;
  @Nullable
  private final String dbUserName;
  @Nullable
  private final String dbPassword;

  private final CouchDBLink dbLink;

  private final ObjectMapper mapper;

  public static synchronized CouchDBTestHelper get()
  {
    if (helper == null)
      helper = new CouchDBTestHelper();
    return helper;
  }

  private CouchDBTestHelper()
  {
    dbUrl = null;
    dbUserName = null;
    dbPassword = null;
    dbLink = new CouchDBLink(dbUrl, dbUserName, dbPassword, TEST_DB);
    mapper = new ObjectMapper();
  }

  public String getDbUrl()
  {
    return dbUrl;
  }

  public String getDbUserName()
  {
    return dbUserName;
  }

  public String getDbPassword()
  {
    return dbPassword;
  }

  public String getDatabase()
  {
    return TEST_DB;
  }

  public CouchDBLink getDbLink()
  {
    return dbLink;
  }

  public ViewQuery createAndFetchViewQuery()
  {
    if (!dbLink.getConnector().contains(DESIGN_DOC_ID)) {
      //The design document doesn't exist in the database so we create it.
      JsonNode rootNode = mapper.createObjectNode();
      ((ObjectNode) rootNode).put("language", "javascript");
      ((ObjectNode) rootNode).putObject("views").putObject(TEST_VIEW).put("map", "function(doc) {\n  emit(doc._id, doc);\n}");
      dbLink.getConnector().create(DESIGN_DOC_ID, rootNode);
    }
    return new ViewQuery().designDocId(DESIGN_DOC_ID).viewName(TEST_VIEW);
  }

  public void insertDocument(Map<String,String> dbTuple)
  {
    String docId = dbTuple.get("_id");
    if (docId != null && dbLink.getConnector().contains(docId)) {
      JsonNode docNode = dbLink.getConnector().get(JsonNode.class, docId);
      if (docNode != null && dbTuple.get("_rev") == null)
        dbTuple.put("_rev",docNode.get("_rev").getTextValue());
    }
    dbLink.getConnector().update(dbTuple);
  }

  public JsonNode fetchDocument(String docId)
  {
    return dbLink.getConnector().get(JsonNode.class, docId);
  }

  public int getTotalDocuments() {
    return dbLink.getConnector().queryView(createAndFetchViewQuery()).getTotalRows();
  }

}
