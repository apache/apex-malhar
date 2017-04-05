package com.example.fileIO;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.io.fs.AbstractThroughputFileInputOperator;

public class ThroughputBasedReader extends AbstractThroughputFileInputOperator<byte[]>
{
  /**
   * prefix for file start and finish control tuples
   */
  public static final char START_FILE = '(', FINISH_FILE = ')';
  private static final int DEFAULT_BLOCK_SIZE = 1024 * 64;

  @OutputPortFieldAnnotation(optional = false)
  public final transient DefaultOutputPort<byte[]> output = new DefaultOutputPort<>();
  @OutputPortFieldAnnotation(optional = false)
  public final transient DefaultOutputPort<String> control = new DefaultOutputPort<>();

  private boolean endOfFile;
  private transient int pauseTime;
  private int blockSize;
  private int blockThreshold;
  private int blocksReadInWindow;

  @Override
  public void setup(Context.OperatorContext context)
  {
    if (blockSize == 0) {
      blockSize = DEFAULT_BLOCK_SIZE;
    }
    if (blockThreshold == 0) {
      blockThreshold = 2;
    }
    super.setup(context);
    pauseTime = context.getValue(Context.OperatorContext.SPIN_MILLIS);
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    blocksReadInWindow = 0;
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    endOfFile = false;
  }

  @Override
  public void emitTuples()
  {
    if (blocksReadInWindow >= blockThreshold) {
      try {
        Thread.sleep(pauseTime);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      return;
    }
    if (!endOfFile) {
      super.emitTuples();
      return;
    }
  }

  @Override
  protected InputStream openFile(Path path) throws IOException
  {
    control.emit(START_FILE + path.getName());
    return super.openFile(path);
  }

  @Override
  protected void closeFile(InputStream is) throws IOException
  {
    endOfFile = true;
    control.emit(new File(currentFile).getName() + FINISH_FILE);
    super.closeFile(is);
  }

  @Override
  protected byte[] readEntity() throws IOException
  {
    byte[] block = new byte[blockSize];
    int bytesRead = inputStream.read(block, 0, blockSize);
    if (bytesRead <= 0) {
      return null;
    }
    blocksReadInWindow++;
    return Arrays.copyOf(block, bytesRead);
  }

  @Override
  protected void emit(byte[] tuple)
  {
    output.emit(tuple);
  }

  public int getBlockSize()
  {
    return blockSize;
  }

  public void setBlockSize(int blockSize)
  {
    this.blockSize = blockSize;
  }

  /**
   * Gets number of blocks to emit per window
   * 
   * @return
   */
  public int getBlockThreshold()
  {
    return blockThreshold;
  }

  /**
   * Sets number of blocks to emit per window
   * 
   * @param blockThreshold
   */
  public void setBlockThreshold(int blockThreshold)
  {
    this.blockThreshold = blockThreshold;
  }
}
