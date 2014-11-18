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
package com.datatorrent.demos.goldengate.utils;

import java.io.Serializable;

import com.goldengate.atg.datasource.meta.TableName;

/**
 * A serializable version of Golden Gate's TableName object.
 */
public class _TableName implements Serializable
{
  private static final long serialVersionUID = -5954542078639907758L;
  private String fullName;
  private String originalName;
  private String originalSchemaName;
  private String originalShortName;
  private String schemaName;
  private String shortName;

  /**
   * Loads the data from the given TableName object.
   * @param tn The Golden Gate TableName object to clone.
   */
  public void readFromTableName(TableName tn)
  {
    fullName = tn.getFullName();
    originalName = tn.getOriginalName();
    originalSchemaName = tn.getOriginalSchemaName();
    originalShortName = tn.getOriginalShortName();
    schemaName = tn.getSchemaName();
    shortName = tn.getShortName();
  }

  public String getFullName()
  {
    return fullName;
  }

  public void setFullName(String fullName)
  {
    this.fullName = fullName;
  }

  public String getOriginalName()
  {
    return originalName;
  }

  public void setOriginalName(String originalName)
  {
    this.originalName = originalName;
  }

  public String getOriginalSchemaName()
  {
    return originalSchemaName;
  }

  public void setOriginalSchemaName(String originalSchemaName)
  {
    this.originalSchemaName = originalSchemaName;
  }

  public String getOriginalShortName()
  {
    return originalShortName;
  }

  public void setOriginalShortName(String originalShortName)
  {
    this.originalShortName = originalShortName;
  }

  public String getSchemaName()
  {
    return schemaName;
  }

  public void setSchemaName(String schemaName)
  {
    this.schemaName = schemaName;
  }

  public String getShortName()
  {
    return shortName;
  }

  public void setShortName(String shortName)
  {
    this.shortName = shortName;
  }

  @Override
  public String toString()
  {
    return "_TableName [fullName=" + fullName + ", originalName=" + originalName + ", originalSchemaName=" + originalSchemaName + ", originalShotName=" + originalShortName + ", schemaName=" + schemaName + ", shortName=" + shortName + "]";
  }

}
