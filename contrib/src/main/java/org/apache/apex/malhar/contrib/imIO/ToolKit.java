package org.apache.apex.malhar.contrib.imIO;
/*
 * imIO4
 * Created by Aditya Gholba on 9/2/17.
 */

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import javax.imageio.ImageIO;
import org.opencv.core.Mat;
import org.opencv.core.MatOfByte;
import org.opencv.imgcodecs.Imgcodecs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.common.util.BaseOperator;



public class ToolKit extends BaseOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(ToolKit.class);
  public static String fileType;
  protected static String soPath = "/home/aditya/opencv-3.2.0/build/lib/libopencv_java320.so";
  public final transient DefaultOutputPort<Data> output = new DefaultOutputPort<>();
  public String filePath;
  public transient BufferedImage bufferedImage = null;
  protected int bufferedImageType;
  ArrayList<Data> dataArrayList = new ArrayList<>();
  private int partitions;
  private transient StreamCodec streamCodec;
  public final transient DefaultInputPort<Data> input = new DefaultInputPort<Data>()
  {

    @Override
    public void process(Data tuple)
    {
      filePath = tuple.fileName;
      fileType = "jpg";
      if (filePath.contains(".png")) {
        fileType = "png";
      }
      if (filePath.contains(".jpg")) {
        fileType = "jpg";
      }
      if (filePath.contains(".jpeg")) {
        fileType = "jpeg";
      }
      if (filePath.contains(".fits")) {
        fileType = "fits";
      }
      if (filePath.contains(".gif")) {
        fileType = "gif";
      }
      if (filePath.contains(".tif")) {
        fileType = "tif";
      }
      LOG.info("file type" + fileType);
      processTuple(tuple);
    }

    @Override
    public StreamCodec<Data> getStreamCodec()
    {
      streamCodec = new ImStreamCodec(partitions);
      return streamCodec;
    }
  };

  public int getPartitions()
  {
    return partitions;
  }

  public void setPartitions(int partitions)
  {
    this.partitions = partitions;
  }

  public String getSoPath()
  {
    return soPath;
  }

  public void setSoPath(String soPath)
  {
    this.soPath = soPath;
  }

  protected Mat readMat(byte[] bytesImage)
  {
    System.load(soPath);
    InputStream src = new ByteArrayInputStream(bytesImage);
    BufferedImage bufferedImage = null;
    try {
      bufferedImage = ImageIO.read(src);
      bufferedImageType = bufferedImage.getType();
    } catch (Exception e) {
      LOG.info(e.getMessage());
    }
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try {
      ImageIO.write(bufferedImage, "png", byteArrayOutputStream);
    } catch (Exception e) {
      LOG.info(e.getMessage());
    }
    byte[] bytes = byteArrayOutputStream.toByteArray();
    return Imgcodecs.imdecode(new MatOfByte(bytes), Imgcodecs.CV_LOAD_IMAGE_UNCHANGED);

  }


  protected byte[] writeMat(Mat destination)
  {
    System.load(soPath);
    BufferedImage bufferedImage1 = new BufferedImage(destination.width(), destination.height(), bufferedImageType);
    byte[] data = new byte[((int)destination.total() * destination.channels())];
    destination.get(0, 0, data);
    byte b;
    for (int i = 0; i < data.length; i = i + 3) {
      b = data[i];
      data[i] = data[i + 2];
      data[i + 2] = b;
    }
    bufferedImage1.getRaster().setDataElements(0, 0, destination.cols(), destination.rows(), data);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try {
      ImageIO.write(bufferedImage1, "png", byteArrayOutputStream);
    } catch (Exception e) {
      LOG.info(e.getMessage());
    }
    return byteArrayOutputStream.toByteArray();

  }

  void processTuple(Data data)
  {

  }


}
