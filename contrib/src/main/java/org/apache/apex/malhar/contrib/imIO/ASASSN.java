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

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import javax.imageio.ImageIO;
import javax.validation.constraints.NotNull;

import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.opencv.imgproc.Imgproc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datatorrent.api.DefaultOutputPort;

import static org.opencv.imgcodecs.Imgcodecs.imread;

public class ASASSN extends imIOHelper
{
  private static final Logger LOG = LoggerFactory.getLogger(ASASSN.class);
  public final transient DefaultOutputPort<Data> outputScore = new DefaultOutputPort<>();
  private int matches = 0;
  private int notMatch = 0;
  private int dense = 0;
  //static {System.load(soPath);}
  private ArrayList<Mat> referenceList = new ArrayList<>();
  private ArrayList<Mat> templateList = new ArrayList<>();
  @NotNull
  private String refPath;

  public String getRefPath()
  {
    return refPath;
  }

  public void setRefPath(String refPath)
  {
    this.refPath = refPath;
  }


  private BufferedImage convertToRGB(BufferedImage image)
  {
    BufferedImage newImage = new BufferedImage(image.getWidth(), image.getHeight(), BufferedImage.TYPE_INT_RGB);
    Graphics2D g = newImage.createGraphics();
    g.drawImage(image, 0, 0, null);
    g.dispose();
    return newImage;
  }

  private void compute(Data data)
  {
    System.load(soPath);
    byte[] bytesImage = data.bytesImage;
    Mat source = readMat(bytesImage);
    BufferedImage bufferedImage = null;
    dense = 0;
    float blackPixels = 0;
    float whitePixels = 0;
    float whitePixelsInGrid = 0;
    float blackPixelsInGrid = 0;
    referenceList.clear();
    ArrayList<String> whiteRange = new ArrayList<>();
    InputStream src = new ByteArrayInputStream(bytesImage);
    try {
      bufferedImage = ImageIO.read(src);
    } catch (Exception e) {
      LOG.debug("Error is " + e.getMessage());
    }
    BufferedImage bufferedImage1 = convertToRGB(bufferedImage);
    for (int y = 0; y < 2048; y = y + 64) {
      for (int x = 0; x < 2048; x = x + 64) {
        for (int i = x; i < x + 64; i++) {
          for (int j = y; j < y + 64; j++) {
            Color c = new Color(bufferedImage1.getRGB(i, j));
            String hex = "#" + Integer.toHexString(c.getRGB()).substring(2);
            if (c.getRed() < 50) {
              blackPixels++;
              blackPixelsInGrid++;
            } else {
              whitePixels++;
              whitePixelsInGrid++;
              if (!whiteRange.contains(hex)) {
                whiteRange.add(hex);
              }
            }

          }
        }
        int totalPixelsInGrid = (int)whitePixelsInGrid + (int)blackPixelsInGrid;
        float whitePixelDensity = whitePixelsInGrid / totalPixelsInGrid;
        if (((whitePixelDensity * 100) > 89 && (whitePixelDensity * 100) < 99) ||
            ((whitePixelDensity * 100) > 60 && (whitePixelDensity * 100) < 70)) { //LOG.info(x+","+y);
          for (int p1 = x; p1 < x + 64; p1++) {
            for (int p2 = y; p2 < y + 64; p2++) {
              Color pink = new Color(255, 104, 150);
              int rgb = pink.getRGB();
              bufferedImage1.setRGB(p1, p2, rgb);
            }
          }
          dense++;

          Mat sub = source.submat(y, y + 64, x, x + 64);
          //writeMat(sub,destination);
          templateList.add(sub);
        }
        whitePixelsInGrid = 0;
        blackPixelsInGrid = 0;

      }
    }

    LOG.info("Black pixels:" + blackPixels);
    LOG.info("White pixels:" + whitePixels);
    LOG.info("White area:" + (whitePixels * 100 / (whitePixels + blackPixels)));
    LOG.info("Black area:" + (blackPixels * 100 / (whitePixels + blackPixels)));
    LOG.info("Total:" + (whitePixels + blackPixels) + " should be :" + (2048 * 2048));
    LOG.info("White hex range size:" + whiteRange.size());
    LOG.info("White hex range:" + whiteRange);
    LOG.info("Dense blocks:" + dense);
    LOG.info("Dense blocks:" + dense + " temList:" + templateList.size());/*
    //LOG.info("matchCalled");
    //firstConcat(data);
    //ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    //try{ImageIO.write(bufferedImage1,"png",byteArrayOutputStream);}catch(Exception e){}
    //data.bytesImage=byteArrayOutputStream.toByteArray();
    */
    if (dense < 512) {
      match(data);
    }

    if (dense > 512) {
      output.emit(data);
    }

  }

  private void match(Data data)
  {
    System.load(soPath);
    int i = 0;
    File[] files = new File(refPath).listFiles();
    double[] mval = new double[templateList.size() * files.length];
    int matches2 = 0;
    for (File file : files) {
      Mat source = imread(file.getAbsoluteFile().toString());
      if (matches < dense) {
        for (Mat template : templateList) {
          String refImagePath = file.getAbsolutePath();
          Mat result = new Mat();
          double threshold = 0.44;
          if (matches > 5) {
            threshold = 0.43;
          } else if (matches > 10) {
            threshold = 0.42;
          } else if (matches > 15) {
            threshold = 0.41;
          } else if (matches > 20) {
            threshold = 0.40;
          }
          //Mat template = templateList.get(i);
          //LOG.info(templateList.indexOf(template));
          if (matches <= 25) {
            Imgproc.matchTemplate(source, template, result, Imgproc.TM_CCOEFF_NORMED);
            //Core.normalize(result, result, 0, 1, Core.NORM_MINMAX, -1, new Mat());
            Core.MinMaxLocResult mmr = Core.minMaxLoc(result);
            //LOG.info(mmr.maxVal );

            {
              mval[i] = mmr.maxVal;
            }
            i++;
            if (mmr.maxVal >= threshold) {
              matches++;
              //LOG.info(mmr.maxVal );
              //LOG.info(refImagePath);
              //LOG.info(templateList.indexOf(template));
            }
            if (mmr.maxVal < threshold) {
              notMatch++;
            }
          }
        }
      }

    }
    templateList.clear();
    Arrays.sort(mval);
    if (matches >= 20) {

      String mValsToString = "";
      if (mval.length > 10) {
        for (int l = mval.length - 1; l > mval.length - 11; l--) {
          mValsToString = mValsToString + mval[l] + " ";
        }

      } else {
        for (int l = mval.length - 1; l > 0; l--) {
          mValsToString = mValsToString + mval[l] + " ";
        }
      }
      LOG.info("Matches C " + data.fileName + " matches " + matches + " dense " + dense + " mvals " + mValsToString);
      outputScore.emit(data);
    } else {
      //data2ArrayList.remove(partData);
      LOG.info("Matches O " + data.fileName + " matches " + matches + " dense " + dense);
      output.emit(data);
    }
    matches = 0;
  }

  private void firstConcat(Data data)
  {
    Mat r2 = new Mat();
    LOG.info("ref list size " + referenceList.size());
    if (referenceList.size() < 1024) {
      Core.hconcat(referenceList, r2);
    } else {
      ArrayList<Mat> subReferenceList = new ArrayList<>(referenceList.subList(0, 1023));
      Core.hconcat(subReferenceList, r2);
    }
    writeMat(r2);
  }

  @Override
  void processTuple(Data data)
  {
    compute(data);
  }
}
