/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.dimensions;

import com.datatorrent.lib.appdata.schemas.Fields;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Set;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class MetaDataDescriptor
{
  private String metadataPrefix;
  private Map<String, String> fieldToMetadata;
  private Fields fields;
  private Fields metadataFields;

  public MetaDataDescriptor(String metadataPrefix,
                            Fields fields)
  {
    setMetaDataPrefix(metadataPrefix);
    setFields(fields);

    initialize();
  }

  private void initialize()
  {
    Set<String> metadata = Sets.newHashSet();
    Set<String> fieldsSet = fields.getFields();
    fieldToMetadata = Maps.newHashMap();

    for(String field: fieldsSet) {
      String metadataField = field;

      do {
        metadataField = metadataPrefix + metadataField;
      } while(fieldsSet.contains(metadataField));

      fieldToMetadata.put(field, metadataField);
      metadata.add(metadataField);
    }

    metadataFields = new Fields(metadata);
  }

  private void setFields(Fields fields)
  {
    Preconditions.checkNotNull(fields);
    this.fields = fields;
  }

  public Fields getFields()
  {
    return fields;
  }

  public Fields getMetadataFields()
  {
    return metadataFields;
  }

  public String getMetadataField(String metadata)
  {
    return fieldToMetadata.get(metadata);
  }

  private void setMetaDataPrefix(String metadataPrefix)
  {
    Preconditions.checkNotNull(metadataPrefix);
    this.metadataPrefix = metadataPrefix;
  }
}
