package com.github.sparkaligner.aligners.bwa;

import com.github.sparkaligner.BaseAligner;

import java.io.File;
import java.io.Serializable;

public class Bwa extends BaseAligner implements Serializable {

  public Bwa(String args[]) {
    super("bwa", new BwaOptions(args));
  }

  public void alignReads(String fastqFileName1, String fastqFileName2) {
    boolean isPairedReads = true;

    if (fastqFileName2 == null) {
      isPairedReads = false;
    }

    this.execute(0, fastqFileName1, fastqFileName2);
    BwaOptions bwaSpecificOptions = (BwaOptions) options;

    // In case of the ALN algorithm, more executions of BWA are needed
    if (bwaSpecificOptions.isAlnAlgorithm()) {

      this.execute(1, fastqFileName1, fastqFileName2);

      if (isPairedReads) {
        this.execute(2, fastqFileName1, fastqFileName2);

        File tmpSaiFile2 = new File(fastqFileName2 + ".sai");
        tmpSaiFile2.delete();
      }

      File tmpSaiFile1 = new File(fastqFileName1 + ".sai");
      tmpSaiFile1.delete();
    }
  }
}
