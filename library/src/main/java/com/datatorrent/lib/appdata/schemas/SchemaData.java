/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.appdata.schemas;

public class SchemaData
{
  private String schemaType;
  private String schemaVersion;

  public SchemaData()
  {
  }

  /**
   * @return the schemaType
   */
  public String getSchemaType()
  {
    return schemaType;
  }

  /**
   * @param schemaType the schemaType to set
   */
  public void setSchemaType(String schemaType)
  {
    this.schemaType = schemaType;
  }

  /**
   * @return the schemaVersion
   */
  public String getSchemaVersion()
  {
    return schemaVersion;
  }

  /**
   * @param schemaVersion the schemaVersion to set
   */
  public void setSchemaVersion(String schemaVersion)
  {
    this.schemaVersion = schemaVersion;
  }
}
