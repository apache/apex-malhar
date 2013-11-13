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


import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.google.common.collect.Maps;
import junit.framework.Assert;
import org.codehaus.jackson.JsonNode;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.util.Map;


/**
 * Test for {@link MapBasedCouchDbOutputOperator}
 *
 * @since 0.3.5
 */
public class CouchDBOutputOperatorTest
{
  private static final Logger LOGGER = LoggerFactory.getLogger(CouchDBOutputOperatorTest.class);


  @Test
  public void testCouchDBOutputOperator() throws MalformedURLException
  {
    String testDocumentId = "TestDocument";
    Map<Object, Object> tuple = Maps.newHashMap();
    tuple.put("_id", testDocumentId);
    tuple.put("name", "TD");
    tuple.put("type", "test");

    MapBasedCouchDbOutputOperator dbOutputOper = new MapBasedCouchDbOutputOperator();
    dbOutputOper.setDatabase(CouchDBTestHelper.get().getDatabase());
    dbOutputOper.setUpdateRevisionWhenNull(true);

    dbOutputOper.setup(new OperatorContextTestHelper.TestIdOperatorContext(1));
    dbOutputOper.beginWindow(0);
    dbOutputOper.inputPort.process(tuple);
    dbOutputOper.endWindow();

    tuple.put("output-type", "map");

    dbOutputOper.beginWindow(1);
    dbOutputOper.inputPort.process(tuple);
    dbOutputOper.endWindow();

    //Test if the document was persisted
    JsonNode docNode = CouchDBTestHelper.get().fetchDocument(testDocumentId);
    Assert.assertNotNull("Document saved", docNode);

    Assert.assertEquals("name of document", "TD", docNode.get("name").getTextValue());
    Assert.assertEquals("type of document", "test", docNode.get("type").getTextValue());
    Assert.assertEquals("output-type", "map", docNode.get("output-type").getTextValue());
  }
}
