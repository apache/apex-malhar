package org.apache.apex.malhar.contrib.imIO;
/*
 * imIO4
 * Created by Aditya Gholba on 23/3/17.
 * Read image from byte[] using readMat(byte[] byteImage)
 * Write image from Mat using writeMat(Mat destination)
 * Write IP logic in ovcFunc()
 *
 */
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import javax.imageio.ImageIO;
import javax.validation.constraints.NotNull;

import org.apache.hadoop.fs.Path;
import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.opencv.imgproc.Imgproc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datatorrent.api.DefaultOutputPort;




import static org.opencv.imgcodecs.Imgcodecs.imread;


public class ASASSN extends ToolKit
{
  protected static final Logger LOG = LoggerFactory.getLogger(ASASSN.class);
  @NotNull
  protected Path SoPath;
  protected String soPath = SoPath.toString();
  public final transient DefaultOutputPort<Data2> outputScore = new DefaultOutputPort<>();
  protected int matches = 0;
  protected transient int notMatch = 0;
  protected int dense = 0;
  //static {System.load(soPath);}
  protected int bufferedImageType;
  protected ArrayList<Mat> referenceList = new ArrayList<>();
  protected ArrayList<Mat> templateList = new ArrayList<>();
  protected String refPath;

  public String getSoPath()
  {
    return soPath;
  }

  public void setSoPath(String soPath)
  {
    this.soPath = soPath;
  }

  public String getRefPath()
  {
    return refPath;
  }

  public void setRefPath(String refPath)
  {
    this.refPath = refPath;
  }

  protected BufferedImage convertToRGB(BufferedImage image)
  {
    BufferedImage newImage = new BufferedImage(image.getWidth(), image.getHeight(), BufferedImage.TYPE_INT_RGB);
    Graphics2D g = newImage.createGraphics();
    g.drawImage(image, 0, 0, null);
    g.dispose();
    return newImage;
  }

  protected void compute(Data data)
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
      LOG.info(e.getMessage());
    }
    BufferedImage bufferedImage1 = convertToRGB(bufferedImage);
    for (int y = 0; y < bufferedImage1.getHeight(); y = y + 64) {
      for (int x = 0; x < bufferedImage1.getWidth(); x = x + 64) {
        for (int i = x; i < x + 64; i++) {
          for (int j = y; j < y + 64; j++) {
            Color c = new Color(bufferedImage1.getRGB(i, j));
            String hex = "#" + Integer.toHexString(c.getRGB()).substring(2);
            //LOG.info("Pixel at "+i+","+j+" "+hex);
            if (hex.equals("#000000") || hex.equals("#191919") || hex.equals("#0c0c0c")) {
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
              //bufferedImage1.setRGB(p1,p2,rgb);
            }
          }
          dense++;

          Mat sub = source.submat(y, y + 64, x, x + 64);
          //writeMat(sub,destination);
          templateList.add(sub);

          //LOG.info(x+" "+y);
          //referenceList.add(sub);

        }
        whitePixelsInGrid = 0;
        blackPixelsInGrid = 0;

      }
    }

    LOG.info("Black pixels:" + blackPixels);
    LOG.info("White pixels:" + whitePixels);/*
        LOG.info("White area:" + (whitePixels * 100 / (whitePixels + blackPixels)));
        LOG.info("Black area:" + (blackPixels * 100 / (whitePixels + blackPixels)));
        LOG.info("Total:" + (whitePixels + blackPixels) + " should be :" + (2048 * 2048));
        LOG.info("White hex range size:" + whiteRange.size());
        LOG.info("White hex range:" + whiteRange);
        */
    //LOG.info("Dense blocks:"+dense);
    LOG.info("Dense blocks:" + dense + " temList:" + templateList.size());
    LOG.info("matchCalled");
    //firstConcat(data);
    if (dense < 512) {
      match(data);
    }

    if (dense > 512) {
      output.emit(data);
    }

  }

  protected void match(Data data)
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
          //Mat template = templateList.get(i);
          //LOG.info(templateList.indexOf(template));
          if (matches <= (dense * 0.20)) {
            Imgproc.matchTemplate(source, template, result, Imgproc.TM_CCOEFF_NORMED);
            //Core.normalize(result, result, 0, 1, Core.NORM_MINMAX, -1, new Mat());
            Core.MinMaxLocResult mmr = Core.minMaxLoc(result);
            //LOG.info(mmr.maxVal );

            {
              mval[i] = mmr.maxVal;
            }
            i++;
            if (mmr.maxVal >= 0.40) {
              matches++;
              //LOG.info(mmr.maxVal );
              //LOG.info(refImagePath);
              //LOG.info(templateList.indexOf(template));
            }
            if (mmr.maxVal < 0.40) {
              notMatch++;
              if (refImagePath.contains(data.fileName)) {
                // LOG.info("Should Match but did NOT! "+refImagePath);
              }
            }
          }
        }
      }
    }
    templateList.clear();
    Data2 data2 = new Data2();
    data2.bytesImage = data.bytesImage;
    data2.fileName = data.fileName;
    data2.matches = matches;
    data2.dense = dense;
    matches = 0;
    outputScore.emit(data2);


  }


  protected void firstConcat(Data data)
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

