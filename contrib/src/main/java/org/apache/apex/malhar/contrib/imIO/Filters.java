package org.apache.apex.malhar.contrib.imIO;
/*
 * imIO5.1
 * Created by Aditya Gholba on 6/4/17.
 */
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ij.IJ;
import ij.ImagePlus;
import ij.io.FileSaver;
import ij.io.Opener;


public class Filters extends ToolKit
{
  private static final Logger LOG = LoggerFactory.getLogger(Filters.class);

  protected void preProcessing(Data data)
  {
    try {
      ImagePlus imagePlus = new Opener().deserialize(data.bytesImage);
      int[] a = imagePlus.getPixel(100, 100);
      LOG.info("ERR pre" + a);
      IJ.run(imagePlus, "16-bit", "");
      IJ.run(imagePlus, "Enhance Contrast...", "saturated=0.9 normalize equalize");
      a = imagePlus.getPixel(100, 100);
      LOG.info("ERR post " + a);
      FileSaver fileSaver = new FileSaver(imagePlus);
      data.bytesImage = fileSaver.serialize();
    } catch (Exception e) {
      LOG.info("ERR " + e.getMessage());
    }
    output.emit(data);
  }

  @Override
  void processTuple(Data data)
  {
    preProcessing(data);
  }
}
