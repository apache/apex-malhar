/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain imageInBytes copy of the License at
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
package org.apache.apex.malhar.contrib.imageIO;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.imageio.ImageIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * This class handles the input and output of all the image processing operators under imageIO.
 * It provides additional read/write methods and sets file path, file name, file type to make sure all operators are
 * compatible with each other.
 * All image processing operators should extend this class and override the processTuple(Data data) method.
 *
 */
public abstract class AbstractImageProcessingOperator extends BaseOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(AbstractImageProcessingOperator.class);
  public static String fileType;

  public final transient DefaultOutputPort<Data> output = new DefaultOutputPort<>();
  public String filePath;
  public transient BufferedImage bufferedImage = null;
  public int bufferedImageType;
  String[] compatibleFileTyes = {"jpg", "png", "jpeg", "fits", "gif", "tif"};
  public final transient DefaultInputPort<Data> input = new DefaultInputPort<Data>()
  {

    @Override
    public void process(Data tuple)
    {
      filePath = tuple.fileName;
      fileType = "jpg";
      for( int i = 0; i < compatibleFileTyes.length; i++)
      {
        if( filePath.contains(compatibleFileTyes[i]))
        {
          fileType = compatibleFileTyes[i];
          if( fileType.equalsIgnoreCase("jpeg"))
          {
            fileType = "jpg";
          }
        }
      }
      LOG.info("file type" + fileType);
      processTuple(tuple);
    }
  };

  public BufferedImage byteArrayToBufferedImage(byte[] imageInBytes)
  {
    byte[] byteImage = imageInBytes;
    InputStream in = new ByteArrayInputStream(byteImage);
    try {
      bufferedImage = ImageIO.read(in);
      in.reset();
      in.close();
    } catch (IOException e) {
      LOG.debug("Error in reading file " + e.getStackTrace());
    }
    return bufferedImage;
  }

  public byte[] bufferedImageToByteArray(BufferedImage bufferedImage)
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      ImageIO.write(bufferedImage, AbstractImageProcessingOperator.fileType, baos);
    } catch (IOException e) {
      LOG.debug("Error in reading file " + e.getStackTrace());
    }
    return baos.toByteArray();
  }

  abstract void processTuple(Data data);
}
