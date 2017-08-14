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
package org.apache.apex.malhar.lib.appdata.snapshot;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.appdata.gpo.GPOMutable;
import org.apache.apex.malhar.lib.appdata.schemas.Schema;
import org.apache.apex.malhar.lib.appdata.schemas.SchemaResult;
import org.apache.apex.malhar.lib.appdata.schemas.SchemaUtils;

import com.google.common.collect.Sets;

public class AppDataSnapshotServerTagsSupportTest
{
  public static class AppDataSnapshotServerSchemaExport extends AbstractAppDataSnapshotServer<Object>
  {
    SchemaResult schemaResult;
    String schemaResultJSON;

    @Override
    public GPOMutable convert(Object inputEvent)
    {
      return null;
    }

    @Override
    public void endWindow()
    {
      while ((schemaResult = schemaQueue.poll()) != null) {
        schemaResultJSON = resultSerializerFactory.serialize(schemaResult);
        queryResult.emit(schemaResultJSON);
      }

      queryProcessor.endWindow();
    }
  }

  private static final String schemaLocation = "satisfactionRatingSnapshotSchema_test.json";
  private static final String TAG = "bulletin";

  private static final String schemaWithTagsLocation = "satisfactionRatingWithTagsSnapshotSchema_test.json";

  @Test
  public void testSchema() throws Exception
  {
    AppDataSnapshotServerSchemaExport[] snapshotServers = getSnapshotServers();

    for (AppDataSnapshotServerSchemaExport snapshotServer : snapshotServers) {
      snapshotServer.setup(null);

      snapshotServer.processQuery("{\"id\":123, \"type\":\"schemaQuery\"}");
      snapshotServer.beginWindow(0L);
      snapshotServer.endWindow();

      String result = snapshotServer.schemaResultJSON;

      JSONObject json = new JSONObject(result);
      JSONObject jsonData = (JSONObject)json.getJSONArray("data").get(0);
      JSONArray tags = jsonData.getJSONArray(Schema.FIELD_SCHEMA_TAGS);
      Assert.assertTrue("No tags", tags != null);
      Assert.assertEquals("Invalid tag.", tags.get(0), TAG);
    }
  }

  protected AppDataSnapshotServerSchemaExport[] getSnapshotServers()
  {
    AppDataSnapshotServerSchemaExport[] snapshotServers = new AppDataSnapshotServerSchemaExport[2];

    {
      String schema = SchemaUtils.jarResourceFileToString(schemaLocation);

      AppDataSnapshotServerSchemaExport snapshotServer = new AppDataSnapshotServerSchemaExport();

      snapshotServer.setSnapshotSchemaJSON(schema);
      snapshotServer.setTags(Sets.newHashSet(TAG));

      snapshotServers[0] = snapshotServer;
    }
    {
      String schema = SchemaUtils.jarResourceFileToString(schemaWithTagsLocation);

      AppDataSnapshotServerSchemaExport snapshotServer = new AppDataSnapshotServerSchemaExport();

      snapshotServer.setSnapshotSchemaJSON(schema);

      snapshotServers[1] = snapshotServer;
    }

    return snapshotServers;
  }
}
