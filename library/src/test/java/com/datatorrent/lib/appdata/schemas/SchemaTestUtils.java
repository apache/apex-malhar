/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class SchemaTestUtils
{
  private SchemaTestUtils()
  {
  }

  public static String[] wrap(String[] strings, String left, String right)
  {
    String[] result = new String[strings.length];

    for(int sc = 0;
        sc < strings.length;
        sc++) {
      result[sc] = left + strings[sc] + right;
    }

    return result;
  }

  public static String[] wrap(String[] strings, String ws)
  {
    String[] result = new String[strings.length];

    for(int sc = 0;
        sc < strings.length;
        sc++) {
      result[sc] = ws + strings[sc] + ws;
    }

    return result;
  }
}
