package org.apache.apex.malhar.contrib.imIO;
/*
 * imIO5.1
 * Created by Aditya Gholba on 3/4/17.
 */
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

public class BytesFileWriterA extends AbstractFileOutputOperator<Data>
{
  private static final transient Logger LOG = LoggerFactory.getLogger(BytesFileWriterA.class);

  public String fileName;
  private boolean eof;

  @Override
  protected byte[] getBytesForTuple(Data tuple)
  {
    eof = true;
    return tuple.bytesImage;
  }

  @Override
  protected String getFileName(Data tuple)
  {
    fileName = tuple.fileName;
    LOG.info("fileName :" + fileName);
    return tuple.fileName;
  }

  @Override
  public void endWindow()
  {

    if (!eof) {
      LOG.info("ERR no eof" + fileName);
      return;
    }
    if (null == fileName) {
      LOG.info("ERR file name is null" + fileName);
      return;
    }
    try {
      finalizeFile(fileName);
      Thread.sleep(100);
    } catch (Exception e) {
      LOG.info("Finalize err " + e.getMessage());
    }
    LOG.info("no ERR ");
    super.endWindow();
    eof = false;
    fileName = null;
  }


}
