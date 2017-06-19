package org.apache.apex.malhar.contrib.imageIO;
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

import java.awt.HeadlessException;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import javax.imageio.ImageIO;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ij.IJ;
import ij.ImagePlus;
import ij.io.Opener;
import ij.process.ImageProcessor;

/**
 * This operator uses ImageJ libraries for converting images to the desired file format.
 * The following image formats are supported jpeg,png,tiff,gif,fits,raw,avi,bmp.
 *
 * Usage:
 * Convert an image to and from any of the supported file formats.
 * by specifying the toFileType property in properties.xml
 *
 */
public class FileFormatConverter extends AbstractImageProcessingOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(FileFormatConverter.class);

  @NotNull
  public String toFileType;

  public String getToFileType()
  {
    return toFileType;
  }

  public void setToFileType(String toFileType)
  {
    this.toFileType = toFileType;
  }

  protected void converter(Data data)
  {
    String fromFileType = AbstractImageProcessingOperator.fileType;
    if (!fromFileType.contains("fit")) {
      LOG.info("toFileType: " + toFileType + " fromFileType " + fromFileType);
      bufferedImage = byteArrayToBufferedImage(data.bytesImage);
      ImagePlus imgPlus = new ImagePlus("source", bufferedImage);
      try {
        IJ.saveAs(imgPlus, toFileType, "");
      } catch (HeadlessException h) {
        LOG.debug(h.getMessage() + "/n");
      }
      ImageProcessor imageProcessor = imgPlus.getProcessor();
      BufferedImage bufferedImage1;
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      try {
        bufferedImage1 = (BufferedImage)imageProcessor.createImage();
        ImageIO.write(bufferedImage1, toFileType, byteArrayOutputStream);
      } catch (Exception e) {
        LOG.debug("ERR " + e.getMessage());
      }
      data.bytesImage = byteArrayOutputStream.toByteArray();
      data.fileName = data.fileName.replace(fromFileType, toFileType);
      LOG.info("fileName " + data.fileName);
      try {
        byteArrayOutputStream.flush();
        byteArrayOutputStream.reset();
        byteArrayOutputStream.close();
        imageProcessor.reset();
      } catch (Exception e) {
        LOG.debug("Error is " + e.getMessage());
      }
      output.emit(data);
    } else {
      LOG.info("itsFits");
      ImagePlus imgPlus = new Opener().deserialize(data.bytesImage);
      try {
        IJ.saveAs(imgPlus, toFileType, "");
      } catch (Exception e) {
        LOG.debug("Error is  " + e.getMessage());
      }
      ImageProcessor imageProcessor = imgPlus.getProcessor();
      BufferedImage bufferedImage1;
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      try {
        bufferedImage1 = (BufferedImage)imageProcessor.createImage();
        ImageIO.write(bufferedImage1, toFileType, byteArrayOutputStream);
      } catch (Exception e) {
        LOG.debug("Error is " + e.getMessage());
      }
      data.bytesImage = byteArrayOutputStream.toByteArray();
      data.fileName = data.fileName.replace(fromFileType, toFileType);
      LOG.info("fileName " + data.fileName);
      try {
        byteArrayOutputStream.flush();
        byteArrayOutputStream.reset();
        byteArrayOutputStream.close();
      } catch (IOException e) {
        LOG.debug("Error is " + e.getMessage());
      }
      output.emit(data);
    }
  }

  @Override
  void processTuple(Data data)
  {
    converter(data);
  }
}
