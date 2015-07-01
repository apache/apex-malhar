/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.couchdb;

import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.ektorp.CouchDbConnector;
import org.ektorp.DbPath;
import org.ektorp.ViewQuery;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;

/**
 * <br>A helper class that setups the couch db for testing
 *
 * @since 0.3.5
 */
public class CouchDBTestHelper
{
  public static final String TEST_DB = "CouchDbTest";
  public static final String DESIGN_DOC_ID = "_design/CouchDbTest";
  public static final String TEST_VIEW = "testView";
  private static CouchDbConnector connector;
  private static final ObjectMapper mapper = new ObjectMapper();


  public static ViewQuery createAndFetchViewQuery()
  {
    if (!connector.contains(DESIGN_DOC_ID)) {
      //The design document doesn't exist in the database so we create it.
      JsonNode rootNode = mapper.createObjectNode();
      ((ObjectNode) rootNode).put("language", "javascript");
      ((ObjectNode) rootNode).putObject("views").putObject(TEST_VIEW).put("map", "function(doc) {\n  emit(doc._id, doc);\n}");
      connector.create(DESIGN_DOC_ID, rootNode);
    }
    return new ViewQuery().designDocId(DESIGN_DOC_ID).viewName(TEST_VIEW);
  }

  public static void insertDocument(Map<String, String> dbTuple)
  {
    connector.create(dbTuple);
  }

   public static void insertDocument(Object dbTuple)
  {
    connector.create(dbTuple);
  }

  public static JsonNode fetchDocument(String docId)
  {
    return connector.get(JsonNode.class, docId);
  }

  public static int getTotalDocuments()
  {
    return connector.queryView(createAndFetchViewQuery()).getTotalRows();
  }

  static void setup()
  {
    StdHttpClient.Builder builder = new StdHttpClient.Builder();
    HttpClient httpClient = builder.build();
    StdCouchDbInstance instance = new StdCouchDbInstance(httpClient);
    DbPath dbPath = new DbPath(TEST_DB);
    if (instance.checkIfDbExists((dbPath))) {
      instance.deleteDatabase(dbPath.getPath());
    }
    connector = instance.createConnector(TEST_DB, true);
  }

  static void teardown()
  {
    StdHttpClient.Builder builder = new StdHttpClient.Builder();
    HttpClient httpClient = builder.build();
    StdCouchDbInstance instance = new StdCouchDbInstance(httpClient);
    DbPath dbPath = new DbPath(TEST_DB);
    if (instance.checkIfDbExists((dbPath))) {
      instance.deleteDatabase(dbPath.getPath());
    }
  }
}
