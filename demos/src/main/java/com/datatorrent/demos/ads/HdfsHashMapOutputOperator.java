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
package com.datatorrent.demos.ads;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.io.fs.AbstractHdfsOutputOperator;

/**
 * Adapter for writing HashMap to HDFS
 * <p>
 * Serializes tuples into a HDFS file.<br/>
 * </p>
 * 
 * 
 */
public class HdfsHashMapOutputOperator extends AbstractHdfsOutputOperator<HashMap<Object, Object>>
{

  /**
   * File name substitution parameter: The logical id assigned to the operator when assembling the DAG.
   */
  public static final String FNAME_SUB_OPERATOR_ID = "operatorId";
  /**
   * File name substitution parameter: Index of part file when a file size limit is specified.
   */
  public static final String FNAME_SUB_PART_INDEX = "partIndex";

  private int operatorId;
  private int index = 0;

  @Override
  public Path nextFilePath()
  {
    Map<String, String> params = new HashMap<String, String>();
    params.put(FNAME_SUB_PART_INDEX, String.valueOf(index));
    params.put(FNAME_SUB_OPERATOR_ID, Integer.toString(operatorId));
    StrSubstitutor sub = new StrSubstitutor(params, "%(", ")");
    index++;
    return new Path(sub.replace(getFilePathPattern().toString()));
  }

  @Override
  public void setup(OperatorContext context)
  {
    operatorId = context.getId();
    super.setup(context);
  }

  @Override
  public byte[] getBytesForTuple(HashMap<Object,Object> t)
  {
    return t.toString().getBytes();
  }

}
