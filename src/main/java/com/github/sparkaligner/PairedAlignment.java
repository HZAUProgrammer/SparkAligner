package com.github.sparkaligner;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;

public class PairedAlignment extends AlignmentBase
    implements Function2<Integer, Iterator<Tuple2<String, String>>, Iterator<String>> {

  public PairedAlignment(SparkContext context, BaseAligner aligner, File originInputFile) {
    super(context, aligner, originInputFile);
  }

  public Iterator<String> call(Integer arg0, Iterator<Tuple2<String, String>> arg1) throws Exception {

    LOG.info("SparkAligner:: Tmp dir: " + this.tmpDir);
    String originInputFileName = MiscUtils.removeExtenstion(this.originInputFile.getName());
    String fastqFileName1 = this.tmpDir + originInputFileName + "-" + this.appId + "-RDD" + arg0 + "_1";
    String fastqFileName2 = this.tmpDir + originInputFileName + "-" + this.appId + "-RDD" + arg0 + "_2";

    LOG.info("SparkAligner:: Writing file: " + fastqFileName1);
    LOG.info("SparkAligner:: Writing file: " + fastqFileName2);

    File FastqFile1 = new File(fastqFileName1);
    File FastqFile2 = new File(fastqFileName2);

    FileOutputStream fos1;
    FileOutputStream fos2;

    BufferedWriter bw1;
    BufferedWriter bw2;

    ArrayList<String> returnedValues = new ArrayList<String>();

    try {
      fos1 = new FileOutputStream(FastqFile1);
      fos2 = new FileOutputStream(FastqFile2);

      bw1 = new BufferedWriter(new OutputStreamWriter(fos1));
      bw2 = new BufferedWriter(new OutputStreamWriter(fos2));

      Tuple2<String, String> newFastqRead;

      while (arg1.hasNext()) {
        newFastqRead = arg1.next();

        bw1.write(newFastqRead._1);
        bw1.newLine();

        bw2.write(newFastqRead._2);
        bw2.newLine();
      }

      bw1.close();
      bw2.close();

      returnedValues = this.runAlignmentProcess(arg0, fastqFileName1, fastqFileName2);

      // Delete temporary files, as they have now been copied to the
      // output directory
      LOG.info("SparkAligner:: Deleting file: " + fastqFileName1);
      FastqFile1.delete();
      LOG.info("SparkAligner:: Deleting file: " + fastqFileName2);
      FastqFile2.delete();

    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      LOG.error(e.toString());
    }

    return returnedValues.iterator();
  }
}
