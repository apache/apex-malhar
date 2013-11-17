/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.io.fs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FSDataInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * An adaptor to read from hdfs text file input operator
 * Read tuple from the text file in HDFS distributed filesystem
 * Read #linesEachWindow lines for each dag window
 *
 * @since 0.3.4
 */
public class HdfsTextFileInputOperator extends AbstractHDFSInputOperator
{

  private int linesEachWindow = 1;

  @OutputPortFieldAnnotation(name = "HDFSOutput")
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();

  private BufferedReader br = null;

  private static final Logger logger = LoggerFactory.getLogger(HdfsTextFileInputOperator.class);

  @Override
  public void activate(OperatorContext ctx)
  {
    super.activate(ctx);
    String file = getFilePath();
    if (file != null) {
      br = new BufferedReader(new InputStreamReader(openFile(getFilePath())));
    }
  }

  @Override
  public void emitTuples(FSDataInputStream stream)
  {
    try {
      int counterForTuple = 0;
      String bufferedString = br.readLine();
      while (bufferedString != null && counterForTuple++ < linesEachWindow) {
        output.emit(bufferedString);
      }
    } catch (IOException e) {
      logger.error("HDFS reader error", e);
    }
  }

  public int getLinesEachWindow()
  {
    return linesEachWindow;
  }

  public void setLinesEachWindow(int linesEachWindow)
  {
    this.linesEachWindow = linesEachWindow;
  }

}
