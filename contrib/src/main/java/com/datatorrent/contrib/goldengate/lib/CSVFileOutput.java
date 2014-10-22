/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.contrib.goldengate.lib;

import com.datatorrent.lib.io.fs.AbstractFSSingleFileWriter;

public class CSVFileOutput extends AbstractFSSingleFileWriter<Employee, Employee>
{
  @Override
  protected byte[] getBytesForTuple(Employee tuple)
  {
    StringBuilder builder = new StringBuilder();
    builder.append(tuple.eid);
    builder.append(",");
    builder.append(tuple.ename);
    builder.append(",");
    builder.append(tuple.did);
    builder.append("\n");

    return builder.toString().getBytes();
  }
}
