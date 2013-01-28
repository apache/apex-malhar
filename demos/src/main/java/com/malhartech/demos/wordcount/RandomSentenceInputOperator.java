/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.wordcount;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.InputOperator;
import java.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class RandomSentenceInputOperator implements InputOperator
{
  private static final Logger logger = LoggerFactory.getLogger(RandomSentenceInputOperator.class);
  public transient DefaultOutputPort<String> output = new DefaultOutputPort<String>(this);
//  public static final String filename = "/home/wzj/Downloads/hadoop-1.0.4/terafile-10M";
  public static final String filename = "samplefile";
  private BufferedReader br;
  private transient DataInputStream in;
  private final int batchSize = 1000;
  private int readedBatch = 0;
  private transient String sentence;
//  public static final int linesInFile = 10000000;
  public static final int linesInFile = 10000;

  @Override
  public void emitTuples()
  {
    try {
      String line;
      int i = 0;
      while ((line = br.readLine()) != null) {
        sentence = line.substring(20, 20 + 78);
        output.emit(sentence);
        if (++i == batchSize) {
          break;
        }
      }
      if (++readedBatch == linesInFile / batchSize) {
        throw new RuntimeException(new InterruptedException("No more tuples to emit!"));
      }
    }
    catch (IOException ex) {
      logger.debug(ex.toString());
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    try {
      if (br != null) {
        return;
      }
      FileInputStream fstream = new FileInputStream(filename);
      in = new DataInputStream(fstream);
      br = new BufferedReader(new InputStreamReader(in));
    }
    catch (FileNotFoundException ex) {
      logger.debug(ex.toString());
      throw new RuntimeException(new InterruptedException("tera data file is not ready!"));
    }
  }

  @Override
  public void teardown()
  {
    try {
      in.close();
    }
    catch (IOException ex) {
      logger.debug(ex.toString());
    }
  }
}
