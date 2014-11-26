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
package com.datatorrent.contrib.hive;

import java.io.File;
import java.util.Iterator;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableInt;

import com.datatorrent.lib.io.fs.AbstractFSWriter;

import com.datatorrent.api.Context.OperatorContext;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HDFSRollingOutputOperator<T> extends AbstractFSWriter<T>
  {
    private transient String outputFileName;
    protected MutableInt partNumber;
    protected String lastFile;
    protected AbstractHiveHDFS hive;
    private static final Logger logger = LoggerFactory.getLogger(HDFSRollingOutputOperator.class);

    public HDFSRollingOutputOperator()
    {
      setMaxLength(128);
    }

     public void getHDFSRollingparameters(){
      Iterator<String> iterFileNames = this.openPart.keySet().iterator();
      if (iterFileNames.hasNext()) {
        lastFile = iterFileNames.next();
        partNumber = this.openPart.get(lastFile);
      }
    }

    public boolean isHDFSLocation(){
      if(fs instanceof RawLocalFileSystem){
      return true;
      }
      return false;
    }

    @Override
    public void setup(OperatorContext context)
    {
      outputFileName = File.separator + "transactions.out.part";
      logger.info("outputfilename is" + outputFileName);
      super.setup(context);
    }

    @Override
    protected void rotateHook(String finishedFile,long windowId)
    {
      hive.filenames.put(finishedFile, windowId);
    }


    @Override
    protected String getFileName(T tuple)
    {
      return outputFileName;
    }

    //implement this function according to tuple you want to pass in Hive
    @Override
    protected byte[] getBytesForTuple(T tuple)
    {
      String hiveTuple = hive.getHiveTuple(tuple);
      return hiveTuple.getBytes();
    }

    protected void updatePartNumber(){
      this.openPart.put(lastFile, partNumber);
    }

  }
