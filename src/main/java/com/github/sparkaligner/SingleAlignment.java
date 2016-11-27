package com.github.sparkaligner;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.Function2;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;

public class SingleAlignment extends AlignmentBase
    implements Function2<Integer, Iterator<String>, Iterator<String>> {

  public SingleAlignment(SparkContext context, BaseAligner aligner, File originInputFile) {
    super(context, aligner, originInputFile);
  }

  public Iterator<String> call(Integer arg0, Iterator<String> arg1) throws Exception {

    LOG.info("SparkAligner:: Tmp dir: " + this.tmpDir);
    String originInputFileName = MiscUtils.removeExtenstion(this.originInputFile.getName());
    String fastqFileName1 = this.tmpDir + originInputFileName + "-" + this.appId + "-RDD" + arg0 + "_1";

    LOG.info("SparkAligner:: Writing file: " + fastqFileName1);

    File FastqFile1 = new File(fastqFileName1);
    FileOutputStream fos1;
    BufferedWriter bw1;

    ArrayList<String> returnedValues = new ArrayList<String>();

    try {
      fos1 = new FileOutputStream(FastqFile1);
      bw1 = new BufferedWriter(new OutputStreamWriter(fos1));

      String newFastqRead;

      while (arg1.hasNext()) {
        newFastqRead = arg1.next();

        bw1.write(newFastqRead);
        bw1.newLine();
      }

      bw1.close();

      returnedValues = this.runAlignmentProcess(arg0, fastqFileName1, null);
      // Delete the temporary file, as is have now been copied to the
      // output directory
      FastqFile1.delete();

    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      LOG.error(e.toString());
    }

    return returnedValues.iterator();
  }
}
