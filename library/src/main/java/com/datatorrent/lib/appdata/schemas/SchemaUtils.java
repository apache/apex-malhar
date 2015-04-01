/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.commons.io.IOUtils;

import java.util.Date;



/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class SchemaUtils
{
  public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");

  private SchemaUtils()
  {
  }

  public static String getDateString(long time)
  {
    return sdf.format(new Date(time));
  }

  public static long getLong(String date)
  {
    try {
      return sdf.parse(date).getTime();
    }
    catch(ParseException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static String jarResourceFileToString(String resource)
  {
    StringWriter stringWriter = new StringWriter();
    try {
      IOUtils.copy(SchemaUtils.class.getClassLoader().getResourceAsStream(resource),
                   stringWriter);
    }
    catch(IOException ex) {
      throw new RuntimeException(ex);
    }
    return stringWriter.toString();
  }

  public static String inputStreamToString(InputStream inputStream)
  {
    StringWriter stringWriter = new StringWriter();

    try {
      IOUtils.copy(inputStream, stringWriter);
    }
    catch(IOException ex) {
      throw new RuntimeException(ex);
    }

    return stringWriter.toString();
  }

  public static void checkDateEx(String date)
  {
    try {
      sdf.parse(date);
    }
    catch(ParseException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static boolean checkDate(String date)
  {
    try {
      checkDateEx(date);
    }
    catch(Exception e) {
      return false;
    }

    return true;
  }
}
