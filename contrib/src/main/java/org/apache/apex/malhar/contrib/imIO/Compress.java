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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Iterator;
import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.MemoryCacheImageOutputStream;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class Compress extends imIOHelper
{
  private static final Logger LOG = LoggerFactory.getLogger(Compress.class);
  @NotNull
  private float compressionRatio;

  public float getCompressionRatio()
  {
    return compressionRatio;
  }

  public void setCompressionRatio(float compressionRatio)
  {
    this.compressionRatio = compressionRatio;
  }

  private void compress(Data data)
  {
    if (data != null) {
      try {
        byte[] byteImage = data.bytesImage;
        InputStream in = new ByteArrayInputStream(byteImage);
        bufferedImage = ImageIO.read(in);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Iterator<ImageWriter> writers = ImageIO.getImageWritersByFormatName(imIOHelper.fileType);
        ImageWriter writer = writers.next();
        writer.setOutput(new MemoryCacheImageOutputStream(baos));

        ImageWriteParam param = writer.getDefaultWriteParam();
        if (param.canWriteCompressed()) {
          param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
          param.setCompressionQuality(compressionRatio);
        }
        writer.write(null, new IIOImage(bufferedImage, null, null), param);
        writer.dispose();
        data.bytesImage = baos.toByteArray();
        baos.flush();
        baos.reset();
        baos.close();
        output.emit(data);
        LOG.debug("sent data from compress");
      } catch (Exception e) {
        LOG.debug("compressError " + e.getMessage());
      }
    }
  }

  @Override
  void processTuple(Data data)
  {
    compress(data);
  }
}
