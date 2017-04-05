package com.example.fileIO;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.datatorrent.api.Context;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.api.DefaultOutputPort;


/**
 * read lines from input file and emit them on output port; a begin-file control tuple
 * is emitted when a file is opened and an end-file control tuple when EOF is reached
 */
public class FileReader extends FileReaderMultiDir
{
  private static final Logger LOG = LoggerFactory.getLogger(FileReader.class);

  /**
   * prefix for file start and finish control tuples
   */
  public static final char START_FILE = '(', FINISH_FILE = ')';

  /**
   * output port for file data
   */
  @OutputPortFieldAnnotation(optional = false)
  public final transient DefaultOutputPort<String> output  = new DefaultOutputPort<>();

  /**
   * output port for control data
   */
  @OutputPortFieldAnnotation(optional = false)
  public final transient DefaultOutputPort<String> control = new DefaultOutputPort<>();

  private transient BufferedReader br = null;

  // Path is not serializable so convert to/from string for persistance
  private transient Path filePath;
  private String filePathStr;

  // set to true when end-of-file occurs, to prevent emission of addditional tuples in current window
  private boolean stop;

  // pause for this many milliseconds after end-of-file
  private transient int pauseTime;

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    SlicedDirectoryScanner sds = (SlicedDirectoryScanner)getScanner();
    LOG.info("setup: directory = {}; scanner: " +
             "(startIndex = {}, endIndex = {}, pIndex = {})", getDirectory(),
             sds.getStartIndex(), sds.getEndIndex(), sds.getpIndex());

    pauseTime = context.getValue(Context.OperatorContext.SPIN_MILLIS);

    if (null != filePathStr) {      // restarting from checkpoint
      filePath = new Path(filePathStr);
    }
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    stop = false;
  }

  @Override
  public void emitTuples()
  {
    if ( ! stop ) {        // normal processing
      super.emitTuples();
      return;
    }

    // we have end-of-file, so emit no further tuples till next window; relax for a bit
    try {
      Thread.sleep(pauseTime);
    } catch (InterruptedException e) {
      LOG.info("Sleep interrupted");
    }
  }

  @Override
  protected InputStream openFile(Path curPath) throws IOException
  {
    LOG.debug("openFile: curPath = {}", curPath);
    filePath = curPath;
    filePathStr = filePath.toString();

    // new file started, send control tuple on control port
    control.emit(START_FILE + filePath.getName());

    InputStream is = super.openFile(filePath);
    br = new BufferedReader(new InputStreamReader(is));
    return is;
  }

  @Override
  protected void closeFile(InputStream is) throws IOException
  {
    LOG.debug("closeFile: filePath = {}", filePath);
    super.closeFile(is);

    // reached end-of-file, send control tuple on control port
    control.emit(filePath.getName() + FINISH_FILE);

    br.close();
    br = null;
    filePath = null;
    filePathStr = null;
    stop = true;
  }

  @Override
  protected String readEntity() throws IOException
  {
    // try to read a line
    final String line = br.readLine();
    if (null != line) {                         // normal case
      LOG.debug("readEntity: line = {}", line);
      return line;
    }

    // end-of-file (control tuple sent in closeFile()
    LOG.info("readEntity: EOF for {}", filePath);
    return null;
  }

  @Override
  protected void emit(String tuple)
  {
    output.emit(tuple);
  }

}
