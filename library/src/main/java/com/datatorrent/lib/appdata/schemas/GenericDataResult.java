/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.qr.DataSerializerInfo;
import com.datatorrent.lib.appdata.qr.DataType;
import com.google.common.base.Preconditions;

import java.util.List;


/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */

@DataType(type=GenericDataQuery.TYPE)
@DataSerializerInfo(clazz=GenericDataResultSerializer.class)
public class GenericDataResult extends GenericDataResultTabular
{
  public static final String TYPE = "dataResult";

  private List<GPOMutable> keys;

  public GenericDataResult(GenericDataQuery dataQuery,
                           List<GPOMutable> keys,
                           List<GPOMutable> values,
                           long countdown)
  {
    super((GenericDataQueryTabular) dataQuery,
          values,
          countdown);
    setKeys(keys);

    if(keys.size() != values.size()) {
      throw new IllegalArgumentException("The keys " + keys.size() +
                                         " and values " + values.size() +
                                         " arrays must be the same size.");
    }
  }


  @Override
  public GenericDataQuery getQuery()
  {
    return (GenericDataQuery) super.getQuery();
  }

  private void setKeys(List<GPOMutable> keys)
  {
    Preconditions.checkNotNull(keys);
    this.keys = keys;
  }

  public List<GPOMutable> getKeys()
  {
    return keys;
  }
}
