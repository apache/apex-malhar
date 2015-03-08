/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.qr;

import com.google.common.collect.Maps;
import java.lang.annotation.Annotation;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class DataSerializerFactory
{
  private static final Logger logger = LoggerFactory.getLogger(DataSerializerFactory.class);

  private Map<Class<? extends Result>, CustomDataSerializer> clazzToCustomResultBuilder = Maps.newHashMap();
  private Map<Class<? extends Result>, String> clazzToType = Maps.newHashMap();

  public DataSerializerFactory()
  {
  }

  public String serialize(Result result)
  {
    CustomDataSerializer mcrs = clazzToCustomResultBuilder.get(result.getClass());
    Class<? extends Result> schema = result.getClass();

    if(mcrs == null) {
      Annotation[] ans = schema.getAnnotations();

      Class<? extends CustomDataSerializer> crs = null;
      String type = null;

      for(Annotation an: ans) {
        if(an instanceof DataSerializerInfo) {
          if(crs != null) {
            throw new UnsupportedOperationException("Cannot specify the "
                    + DataSerializerInfo.class
                    + " annotation twice on the class: "
                    + schema);
          }

          crs = ((DataSerializerInfo)an).clazz();
        }
        else if(an instanceof DataType) {
          if(type != null) {
            throw new UnsupportedOperationException("Cannot specify the " +
                                                    DataType.class +
                                                    " annotation twice on the class: " +
                                                    schema);
          }

          type = ((DataType) an).type();
        }
      }

      if(crs == null) {
        throw new UnsupportedOperationException("No " + DataSerializerInfo.class
                + " annotation found on class: "
                + schema);
      }

      if(type == null) {
        throw new UnsupportedOperationException("No " + DataType.class +
                                                " annotation found on class " + schema);
      }

      try {
        mcrs = crs.newInstance();
      }
      catch(InstantiationException ex) {
        throw new RuntimeException(ex);
      }
      catch(IllegalAccessException ex) {
        throw new RuntimeException(ex);
      }

      clazzToCustomResultBuilder.put(schema, mcrs);
      clazzToType.put(schema, type);
    }

    result.setType(clazzToType.get(schema));

    return mcrs.serialize(result);
  }
}
