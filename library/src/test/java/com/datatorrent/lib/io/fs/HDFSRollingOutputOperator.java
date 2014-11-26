/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.io.fs;

import com.datatorrent.api.Context.OperatorContext;
import java.io.File;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableInt;

public class HDFSRollingOutputOperator<T> extends AbstractFSWriter<T>
  {
    private transient String outputFileName;
    protected MutableInt partNumber;
    protected MutableLong offset;
    protected String lastFile;
    protected HiveInsertOperator hive;
   // private  final Logger logger = LoggerFactory.getLogger(HDFSRollingOutputOperator.class);
    public HDFSRollingOutputOperator()
    {
      setMaxLength(128);
    }




    @Override
    public void setup(OperatorContext context)
    {
      outputFileName = File.separator + "transactions.out.part";
      super.setup(context);
    }

    @Override
    protected void rotateHook(String finishedFile,long windowId)
    {
      //logger.info("finished file is" + finishedFile);
     // logger.info("window in which the part finished" + windowId);
    //  hive.processHiveFile(finishedFile);
      //logger.info("windowIdofFinishedFile is" + windowId);
      hive.filenames.put(finishedFile, windowId);
     // logger.info("files in queue {}" , hive.filenames.keySet());
    }


    @Override
    protected String getFileName(T tuple)
    {
      return outputFileName;
    }

    @Override
    protected byte[] getBytesForTuple(T tuple)
    {
      String hiveTuple = hive.getHiveTuple(tuple);
      //logger.info("hiveTuple is" + hiveTuple);
      return hiveTuple.getBytes();
    }
  }