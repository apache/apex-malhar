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

import java.io.File;
import java.util.ArrayList;
import javax.validation.constraints.NotNull;
import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.opencv.core.Point;
import org.opencv.core.Scalar;
import org.opencv.imgproc.Imgproc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.opencv.imgcodecs.Imgcodecs.imread;
import static org.opencv.imgproc.Imgproc.rectangle;

public class Template_Match extends imIOHelper
{
  protected static final Logger LOG = LoggerFactory.getLogger(Template_Match.class);
  @NotNull
  double matchThreshold;
  private ArrayList<Templates> templatesList = new ArrayList<>();
  private ArrayList<Templates> templatesCopyList = new ArrayList<>();
  private Boolean templatesExists = false;
  private String templatePath;
  private double uniqueThreshold;
  private int objects = 0;

  public Boolean getTemplatesExists()
  {
    return templatesExists;
  }

  public void setTemplatesExists(Boolean templatesExists)
  {
    this.templatesExists = templatesExists;
  }

  public String getTemplatePath()
  {
    return templatePath;
  }

  public void setTemplatePath(String templatePath)
  {
    this.templatePath = templatePath;
  }

  public double getThreshold()
  {
    return matchThreshold;
  }

  public void setThreshold(double threshold)
  {
    this.matchThreshold = threshold;
  }

  public double getUniqueThreshold()
  {
    return uniqueThreshold;
  }

  public void setUniqueThreshold(double uniqueThreshold)
  {
    this.uniqueThreshold = uniqueThreshold;
  }

  private void recognizeWithLiveTemplates(Data data)
  {

    Mat source = readMat(data.bytesImage);
    if (templatesList.size() == 0) {
      objects++;
      Templates faces2 = new Templates();
      faces2.mat = source.clone();
      faces2.name = "person_" + Integer.toString(objects);
      templatesList.add(faces2);
      data.fileName = faces2.name + "_" + data.fileName + ".png";
      data.bytesImage = writeMat(source);
      output.emit(data);
    } else {

      double[] maxValues;
      maxValues = new double[templatesList.size()];
      int numValues = 0;
      Boolean newObject = false;
      for (Templates tempObj : templatesList) {

        Mat template = tempObj.mat;

        Core.MinMaxLocResult mmr = templateMatch(source, template);
        if (mmr.maxVal >= matchThreshold) {
          LOG.info("Max val " + mmr.maxVal);
          data.fileName = tempObj.name + "_" + data.fileName + ".png";
          data.bytesImage = writeMat(source);
          output.emit(data);
          LOG.info("Object is " + data.fileName);

        }
        if (mmr.maxVal < uniqueThreshold) {
          numValues++;
        }
        if (numValues == (templatesList.size())) {
          newObject = true;
        }
      }
      if (newObject) {
        objects++;
        LOG.info("Smallest max values are " + maxValues[0]);
        LOG.info("New Object value " + objects);
        Templates templates = new Templates();
        templates.mat = source.clone();
        templates.name = "Object_" + Integer.toString(objects);
        templatesCopyList.add(templates);
        data.fileName = templates.name + "_" + data.fileName + ".png";
        data.bytesImage = writeMat(source);
        output.emit(data);
      }

      for (Templates templates : templatesCopyList) {
        if (!templatesList.contains(templates)) {
          templatesList.add(templates);
        }
      }
      templatesCopyList.clear();
    }
  }

  private void recognizeWithExistingTemplates(Data data)
  {
    Mat source = readMat(data.bytesImage);
    File[] files = new File(templatePath).listFiles();
    templatesList.clear();
    for (File file : files) {
      Templates templates = new Templates();
      templates.mat = imread(file.getAbsoluteFile().toString());
      templatesList.add(templates);
    }
    //double[] mValues = new double[templatesList.size()*files.length];
    for (Templates tempObj : templatesList) {

      Mat template = tempObj.mat;
      Core.MinMaxLocResult minMaxLocResult = templateMatch(source, template);
      if (minMaxLocResult.maxVal >= matchThreshold) {
        data.fileName = tempObj.name + "_" + data.fileName + ".png";
        data.bytesImage = writeMat(source);
        output.emit(data);
        LOG.info("Object is " + data.fileName);

      }
    }

  }


  protected Core.MinMaxLocResult templateMatch(Mat source, Mat template)
  {
    Mat result = new Mat();
    Imgproc.matchTemplate(source, template, result, Imgproc.TM_CCOEFF_NORMED);
    Core.MinMaxLocResult mmr = Core.minMaxLoc(result);
    Point matchLoc;
    matchLoc = mmr.maxLoc;
    rectangle(source, matchLoc, new Point(matchLoc.x + template.cols(),
        matchLoc.y + template.rows()), new Scalar(0, 255, 0), 10);
    return mmr;
  }


  @Override
  void processTuple(Data data)
  {
    if (templatesExists) {
      recognizeWithExistingTemplates(data);
    } else {
      recognizeWithLiveTemplates(data);
    }
  }
}
