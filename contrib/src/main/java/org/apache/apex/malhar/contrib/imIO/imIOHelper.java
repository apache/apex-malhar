/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.apex.malhar.contrib.imIO;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import javax.imageio.ImageIO;
import javax.validation.constraints.NotNull;
import org.opencv.core.Mat;
import org.opencv.core.MatOfByte;
import org.opencv.imgcodecs.Imgcodecs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.common.util.BaseOperator;



public class imIOHelper extends BaseOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(imIOHelper.class);
  public static String fileType;

  public String getSoPath()
  {
    return soPath;
  }

  public void setSoPath(String soPath)
  {
    this.soPath = soPath;
  }

  @NotNull
  public String soPath;
  public final transient DefaultOutputPort<Data> output = new DefaultOutputPort<>();
  public String filePath;
  public transient BufferedImage bufferedImage = null;
  public int bufferedImageType;
  //Planed ArrayList<Data> dataArrayList = new ArrayList<>();
  private int partitions;
  public transient StreamCodec streamCodec;
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


  protected Mat readMat(byte[] bytesImage)
  {
    System.load(soPath);
    InputStream src = new ByteArrayInputStream(bytesImage);
    BufferedImage bufferedImage = null;
    try {
      bufferedImage = ImageIO.read(src);
      bufferedImageType = bufferedImage.getType();
    } catch (Exception e) {
      LOG.debug("Error is " + e.getMessage());
    }
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try {
      ImageIO.write(bufferedImage, "png", byteArrayOutputStream);
    } catch (Exception e) {
      LOG.debug("Error is " + e.getMessage());
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
      LOG.debug("Error is " + e.getMessage());
    }
    return byteArrayOutputStream.toByteArray();

  }

  void processTuple(Data data)
  {

  }


}
