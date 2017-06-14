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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.coobird.thumbnailator.Thumbnails;

public class Resize extends imIOHelper
{
  private static final Logger LOG = LoggerFactory.getLogger(Resize.class);
  private int width = 0;
  private int height = 0;
  private double scale = 1;

  public double getScale()
  {
    return scale;
  }

  public void setScale(double scale)
  {
    this.scale = scale;
  }

  public int getWidth()
  {
    return width;
  }

  public void setWidth(int width)
  {
    this.width = width;
  }

  public int getHeight()
  {
    return height;
  }

  public void setHeight(int height)
  {
    this.height = height;
  }

  protected void resize(Data data)
  {
    try {
      byte[] byteImage = data.bytesImage;
      InputStream in = new ByteArrayInputStream(byteImage);
      bufferedImage = ImageIO.read(in);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      BufferedImage resizedImage;
      LOG.info("fileTypeIs:" + imIOHelper.fileType + " scale:" + scale);
      //BufferedImage resizedImage = Thumbnails.of(bufferedImage).size(width, height).asBufferedImage();
      if (height == width && width == 0) {
        resizedImage = Thumbnails.of(bufferedImage).scale(scale).asBufferedImage();
      } else {
        resizedImage = Thumbnails.of(bufferedImage).size(width, height).asBufferedImage();
      }
      ImageIO.write(resizedImage, imIOHelper.fileType, baos);
      data.bytesImage = baos.toByteArray();
      output.emit(data);
    } catch (Exception e) {
      LOG.debug("Error is " + e.getMessage());
    }
  }

  @Override
  void processTuple(Data data)
  {
    resize(data);
  }

}
