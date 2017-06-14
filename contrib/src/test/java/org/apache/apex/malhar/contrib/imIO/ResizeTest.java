package org.apache.apex.malhar.contrib.imIO;/*
 * malhar
 * Created by Aditya Gholba on 13/6/17.
 */
import java.io.File;
import javax.imageio.ImageIO;

import org.junit.Before;
import org.junit.Test;
import org.opencv.core.Mat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.opencv.imgcodecs.Imgcodecs.imread;

public class ResizeTest extends Resize
{
  Data data = new Data();
  String filePath;
  private static final Logger LOG = LoggerFactory.getLogger(ResizeTest.class);

  @Before
  public void setup()
  {
    soPath = "/home/aditya/opencv-3.2.0/build/lib/libopencv_java320.so";
    filePath = "/home/aditya/Pictures/Wallpapers2/4k2.jpg";
    File file = new File(filePath);
    try {
      bufferedImageType = ImageIO.read(file).getType();
    } catch (Exception e) {
      LOG.info("Error is " + e.getMessage());
    }
    //SoPath = new Path("file:///home/aditya/opencv-3.2.0/build/lib/libopencv_java320.so");
    System.load(soPath);
    Mat source;
    source = imread(filePath);
    data.bytesImage = writeMat(source);
    data.fileName = "Test.png";
    if (filePath.contains(".png")) {
      imIOHelper.fileType = "png";
    }
    if (filePath.contains(".jpg")) {
      imIOHelper.fileType = "jpg";
    }
    if (filePath.contains(".jpeg")) {
      imIOHelper.fileType = "jpeg";
    }
    if (filePath.contains(".fits")) {
      imIOHelper.fileType = "fits";
    }
    if (filePath.contains(".gif")) {
      imIOHelper.fileType = "gif";
    }
    if (filePath.contains(".tif")) {
      imIOHelper.fileType = "tif";
    }
  }

  @Test
  public void resizeTest()
  {
    ResizeTest resizeTest = new ResizeTest();
    resizeTest.scale = 0.05f;
    resizeTest.resize(data);
  }
}
