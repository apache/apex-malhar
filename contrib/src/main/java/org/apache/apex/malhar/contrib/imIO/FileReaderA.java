package org.apache.apex.malhar.contrib.imIO;
/*
 * imIO5.1
 * Created by Aditya Gholba on 3/4/17.
 */

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.io.fs.AbstractFileInputOperator;
import ij.ImagePlus;
import ij.io.FileSaver;



public class FileReaderA extends AbstractFileInputOperator<Data>
{
  public static final char START_FILE = '(';
  public static final char FINISH_FILE = ')';
  private static final Logger LOG = LoggerFactory.getLogger(FileReaderA.class);
  public final transient DefaultOutputPort<Data> output = new DefaultOutputPort<>();
  public int countImageSent = 0;
  public int countImageSent2 = 0;
  public transient String filePathStr;
  byte[] a;
  private transient BufferedImage br = null;
  private boolean stop;
  private transient int pauseTime;
  private transient Path filePath;
  private Boolean slowDown = false;
  private Boolean extraSlowDown = false;
  private long slowDownMills = 10000;

  public Boolean getSlowDown()
  {
    return slowDown;
  }

  public void setSlowDown(Boolean slowDown)
  {
    this.slowDown = slowDown;
  }

  public long getSlowDownMills()
  {
    return slowDownMills;
  }

  public void setSlowDownMills(long slowDownMills)
  {
    this.slowDownMills = slowDownMills;
  }

  public Boolean getExtraSlowDown()
  {
    return extraSlowDown;
  }

  public void setExtraSlowDown(Boolean extraSlowDown)
  {
    this.extraSlowDown = extraSlowDown;
  }
  //public final transient DefaultOutputPort<Data> output1  = new DefaultOutputPort<>();
  //public final transient DefaultOutputPort<Data> output2  = new DefaultOutputPort<>();

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    pauseTime = context.getValue(Context.OperatorContext.SPIN_MILLIS);

    if (null != filePathStr) {      // restarting from checkpoint
      filePath = new Path(filePathStr);
    }
  }

  @Override
  public void emitTuples()
  {
    if (!stop) {        // normal processing
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
  public void endWindow()
  {
    super.endWindow();
    stop = false;
  }

  @Override
  protected InputStream openFile(Path curPath) throws IOException
  {
    LOG.debug("openFile: curPath = {}", curPath);
    filePath = curPath;
    filePathStr = filePath.toString();
    LOG.info("readOpen " + START_FILE + filePath.getName());
    InputStream is = super.openFile(filePath);
    if (!filePathStr.contains(".fits")) {
      a = IOUtils.toByteArray(is);
    } else {
      String fitsPath = filePath.getParent().toString() + "/" + filePath.getName();
      if (fitsPath.contains(":")) {
        fitsPath = fitsPath.replace("file:", "");
      }
      LOG.info("ERR " + filePath.getParent() + "/" + filePath.getName());
      LOG.info("ERR " + fitsPath);
      ImagePlus imagePlus = new ImagePlus(fitsPath);
      a = new FileSaver(imagePlus).serialize();
    }
    return is;
  }

  @Override
  protected void closeFile(InputStream is) throws IOException
  {
    LOG.debug("closeFile: filePath = {}", filePath);
    super.closeFile(is);
    LOG.info("readClose " + filePath.getName() + FINISH_FILE);
    filePath = null;
    stop = true;
  }

  @Override
  protected Data readEntity() throws IOException
  {
    //try{Thread.sleep(500);}catch (Exception e){LOG.info("Read Sleep"+e.getMessage());}
    LOG.info("read entity was called" + currentFile);
    byte[] imageInByte = a;
    if (countImageSent < 1) {
      countImageSent++;
      //LOG.info("returned Image "+countImageSent+" s"+baos.size()+" br "+currentFile);
      Data data = new Data();
      data.bytesImage = imageInByte;
      data.fileName = filePath.getName().toString();
      return data;
    }
    LOG.info("readEntity: EOF for {}", filePath);
    countImageSent = 0;
    return null;
  }

  @Override
  protected void emit(Data data)
  {
    if (slowDown) {
      Boolean loop = true;
      long startTime = System.currentTimeMillis();
      if (extraSlowDown) {
        if (countImageSent2 % 30 == 0 && countImageSent2 != 0) {
          while (loop == true) {
            long currentTime = System.currentTimeMillis();
            if (currentTime - startTime >= 60000) {
              loop = false;
            }
          }
        }

        if (countImageSent2 % 10 == 0 && countImageSent2 != 0) {
          long temp = slowDownMills / 100;
          temp = temp * 20;
          slowDownMills = slowDownMills + temp;

        }
        if (countImageSent2 % 90 == 0 && countImageSent2 != 0) {
          slowDownMills = 1000;

        }
      }
      while (loop == true) {
        long currentTime = System.currentTimeMillis();
        if (currentTime - startTime >= slowDownMills) {
          loop = false;
        }
      }
    }
    LOG.info("send data from read " + countImageSent2);
    output.emit(data);
    countImageSent2++;
    //output1.emit(data);
    //output2.emit(data);

  }

}

