package com.github.sparkaligner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkContext;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

abstract class AlignmentBase implements Serializable {

  protected static final long serialVersionUID = 1L;
  static final Log LOG = LogFactory.getLog(AlignmentBase.class);

  private String appName = "";
  String appId = "";
  String tmpDir = "";
  private BaseAligner aligner;
  File originInputFile;

  AlignmentBase(SparkContext context, BaseAligner aligner, File originInputFile) {

    this.appId = context.applicationId();
    this.appName = context.appName();
    this.tmpDir = context.getLocalProperty("spark.local.dir");
    this.aligner = aligner;
    this.originInputFile = originInputFile;

    if (this.tmpDir == null) {
      this.tmpDir = "/tmp/";
    } else if (this.tmpDir.startsWith("file:")) {
      this.tmpDir = this.tmpDir.replaceFirst("file:", "");
    }

    LOG.info("SparkAligner:: " + this.appId + " - " + this.appName);
  }

  private ArrayList<String> copyResults(String outputSamFileName) {
    ArrayList< String> returnedValues = new ArrayList<String>();
    String baseOutputDir = this.originInputFile.getParent();
    File outputDir = new File(baseOutputDir, "sparkbwa-out-" + appId);

    if (!outputDir.exists()) {
      outputDir.mkdir();
    }

    String localOutputFile = this.aligner.options.getOutputFile();
    LOG.info(localOutputFile);

    LOG.info("SparkAligner:: " + this.appId + " - " + this.appName + " Copying files...");
    try {
        File localSamOutput = new File(localOutputFile);
        Files.copy(Paths.get(localSamOutput.getPath()), Paths.get(outputDir.getPath(), localSamOutput.getName()));
    } catch (IOException e) {
      e.printStackTrace();
      LOG.error(e.toString());
    }

    // Delete the old results file
    File tmpSamFullFile = new File(localOutputFile);
    tmpSamFullFile.delete();

    returnedValues.add(outputDir + "/" + outputSamFileName);

    return returnedValues;
  }

  private String getOutputSamFilename(Integer readBatchID) {
    return this.appName + "-" + this.appId + "-" + this.originInputFile.getName() + "-" + readBatchID + ".sam";
  }

  ArrayList<String> runAlignmentProcess(Integer readBatchID, String fastqFileName1, String fastqFileName2) {
    String outputSamFileName = this.getOutputSamFilename(readBatchID);
    this.aligner.options.setOutputFile(this.tmpDir + outputSamFileName);
    this.aligner.alignReads(fastqFileName1, fastqFileName2);

    return this.copyResults(outputSamFileName);
  }
}
