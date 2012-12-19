/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class JDBCOperatorTestHelper
{
  private int columnCount = 7;
  public String[] hashMapping1 = new String[7];
  public String[] hashMapping2 = new String[7];
  public String[] hashMapping3 = new String[7];
  public String[] arrayMapping1 = new String[7];
  public String[] arrayMapping2 = new String[7];
  public String[] arrayMapping3 = new String[7];
  public String[] hashMapping4 = new String[7];

  public void buildDataset()
  {
    // mapping1
    hashMapping1[0] = "prop1:col1:INTEGER";
    hashMapping1[1] = "prop2:col2:INTEGER";
    hashMapping1[2] = "prop5:col5:INTEGER";
    hashMapping1[3] = "prop6:col4:INTEGER";
    hashMapping1[4] = "prop7:col7:INTEGER";
    hashMapping1[5] = "prop3:col6:INTEGER";
    hashMapping1[6] = "prop4:col3:INTEGER";

    // mapping2
    hashMapping2[0] = "prop1:col1:INTEGER";
    hashMapping2[1] = "prop2:col2:VARCHAR(10)";
    hashMapping2[2] = "prop5:col5:INTEGER";
    hashMapping2[3] = "prop6:col4:varchar(10)";
    hashMapping2[4] = "prop7:col7:integer";
    hashMapping2[5] = "prop3:col6:VARCHAR(10)";
    hashMapping2[6] = "prop4:col3:int";

    // mapping3
    hashMapping3[0] = "prop1:col1:INTEGER";
    hashMapping3[1] = "prop2:col2:BIGINT";
    hashMapping3[2] = "prop5:col5:CHAR";
    hashMapping3[3] = "prop6:col4:DATE";
    hashMapping3[4] = "prop7:col7:DOUBLE";
    hashMapping3[5] = "prop3:col6:VARCHAR(10)";
    hashMapping3[6] = "prop4:col3:DATE";

    // mapping 4
    arrayMapping1[0] = "col1:INTEGER";
    arrayMapping1[1] = "col2:BIGINT";
    arrayMapping1[2] = "col5:CHAR";
    arrayMapping1[3] = "col4:DATE";
    arrayMapping1[4] = "col7:double";
    arrayMapping1[5] = "col6:VARCHAR(10)";
    arrayMapping1[6] = "col3:DATE";

    // mapping 5
    arrayMapping2[0] = "col1:integer";
    arrayMapping2[1] = "col2:BAD_COLUMN_TYPE";
    arrayMapping2[2] = "col5:char";
    arrayMapping2[3] = "col4:DATE";
    arrayMapping2[4] = "col7:DOUBLE";
    arrayMapping2[5] = "col6:VARCHAR(10)";
    arrayMapping2[6] = "col3:date";

    // mapping 6
    arrayMapping3[0] = "col1:INTEGER";
    arrayMapping3[1] = "col2:BIGINT";
    arrayMapping3[2] = "col5:char";
    arrayMapping3[3] = "col4:DATE";
    arrayMapping3[4] = "col7:DOUBLE";
    arrayMapping3[5] = "col6:VARCHAR(10)";
    arrayMapping3[6] = "col3:date";

    //Multi table mapping
    hashMapping4[0] = "prop1:t1.col1:INTEGER";
    hashMapping4[1] = "prop2:t3.col2:BIGINT";
    hashMapping4[2] = "prop5:t3.col5:CHAR";
    hashMapping4[3] = "prop6:t2.col4:DATE";
    hashMapping4[4] = "prop7:t1.col7:DOUBLE";
    hashMapping4[5] = "prop3:t2.col6:VARCHAR(10)";
    hashMapping4[6] = "prop4:t1.col3:DATE";


  }

  public HashMap<String, Object> hashMapData(String[] mapping, int i)
  {
    HashMap<String, Object> hm = new HashMap<String, Object>();
    for (int j = 1; j <= columnCount; ++j) {
      String[] parts = mapping[j - 1].split(":");
      if (parts[2].toUpperCase().contains("VARCHAR")) {
        parts[2] = "VARCHAR";
      }
      if ("INTEGER".equalsIgnoreCase(parts[2])
              || "INT".equalsIgnoreCase(parts[2])) {
        hm.put(parts[0], new Integer((columnCount * i) + j));
      }
      else if ("BIGINT".equalsIgnoreCase(parts[2])) {
        hm.put(parts[0], new Integer((columnCount * i) + j));
      }
      else if ("CHAR".equalsIgnoreCase(parts[2])) {
        hm.put(parts[0], 'a');
      }
      else if ("DATE".equalsIgnoreCase(parts[2])) {
        hm.put(parts[0], new Date());
      }
      else if ("DOUBLE".equalsIgnoreCase(parts[2])) {
        hm.put(parts[0], new Double((columnCount * i + j) / 3.0));
      }
      else if ("VARCHAR".equalsIgnoreCase(parts[2])) {
        hm.put(parts[0], "Test");
      }
      else if ("TIME".equalsIgnoreCase(parts[2])) {
        hm.put(parts[0], new Date());
      }
      else {
        throw new RuntimeException("Exception while generating data for hashMap");
      }
    }

    return hm;
  }

  public ArrayList<Object> arrayListData(String[] mapping, int i)
  {
    ArrayList<Object> al = new ArrayList<Object>();
    for (int j = 1; j <= columnCount; ++j) {
      String[] parts = mapping[j - 1].split(":");
      if (parts[1].toUpperCase().contains("VARCHAR")) {
        parts[1] = "VARCHAR";
      }
      if ("INTEGER".equalsIgnoreCase(parts[1])) {
        al.add(new Integer((columnCount * i) + j));
      }
      else if ("BIGINT".equalsIgnoreCase(parts[1])) {
        al.add(new Integer((columnCount * i) + j));
      }
      else if ("CHAR".equalsIgnoreCase(parts[1])) {
        al.add('a');
      }
      else if ("DATE".equalsIgnoreCase(parts[1])) {
        al.add(new Date());
      }
      else if ("DOUBLE".equalsIgnoreCase(parts[1])) {
        al.add(new Double((columnCount * i + j) / 3.0));
      }
      else if ("VARCHAR".equalsIgnoreCase(parts[1])) {
        al.add("Test");
      }
      else if ("TIME".equalsIgnoreCase(parts[1])) {
        al.add(new Date());
      }
      else if ("BAD_COLUMN_TYPE".equalsIgnoreCase(parts[1])) {
        al.add(new Integer((columnCount * i) + j));
      }
      else {
        throw new RuntimeException("Exception while generating data for arrayList");
      }
    }

    return al;
  }
}
