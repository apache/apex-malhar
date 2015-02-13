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
public class ResultSerializerFactory
{
  private static final Logger logger = LoggerFactory.getLogger(ResultSerializerFactory.class);

  private Map<Class<? extends Result>, CustomResultSerializer> clazzToCustomResultBuilder = Maps.newHashMap();
  private Map<Class<? extends Result>, String> clazzToType = Maps.newHashMap();

  public ResultSerializerFactory()
  {
  }

  public String serialize(Result result)
  {
    CustomResultSerializer mcrs = clazzToCustomResultBuilder.get(result.getClass());
    Class<? extends Result> schema = result.getClass();

    if(mcrs == null) {
      Annotation[] ans = schema.getAnnotations();

      Class<? extends CustomResultSerializer> crs = null;
      String type = null;

      for(Annotation an: ans) {
        if(an instanceof ResultSerializerInfo) {
          if(crs != null) {
            throw new UnsupportedOperationException("Cannot specify the "
                    + ResultSerializerInfo.class
                    + " annotation twice on the class: "
                    + schema);
          }

          crs = ((ResultSerializerInfo)an).clazz();
        }
        else if(an instanceof QRType) {
          if(type != null) {
            throw new UnsupportedOperationException("Cannot specify the " +
                                                    QRType.class +
                                                    " annotation twice on the class: " +
                                                    schema);
          }

          type = ((QRType) an).type();
        }
      }

      if(crs == null) {
        throw new UnsupportedOperationException("No " + ResultSerializerInfo.class
                + " annotation found on class: "
                + schema);
      }

      if(type == null) {
        throw new UnsupportedOperationException("No " + QRType.class +
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
