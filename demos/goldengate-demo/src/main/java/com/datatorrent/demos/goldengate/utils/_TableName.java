package com.datatorrent.demos.goldengate.utils;

import java.io.Serializable;

import com.goldengate.atg.datasource.meta.TableName;

public class _TableName implements Serializable
{

  /**
   * 
   */
  private static final long serialVersionUID = -5954542078639907758L;
  private String fullName;
  private String originalName;
  private String originalSchemaName;
  private String originalShortName;
  private String schemaName;
  private String shortName;

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
