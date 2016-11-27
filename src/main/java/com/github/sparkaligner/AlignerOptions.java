package com.github.sparkaligner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Serializable;
import java.util.ArrayList;

public abstract class AlignerOptions implements Serializable {

  protected static final Log LOG = LogFactory.getLog(AlignerOptions.class);

  private String alignerExtraArgs = "";

  private String indexPath = "";
  private String inputPath = "";
  private int partitionNumber = 0;
  private String outputFile = "";

  protected String getIndexPath() {
    return indexPath;
  }

  protected void setIndexPath(String indexPath) {
    this.indexPath = indexPath;
  }

  String getInputPath() {
    return inputPath;
  }

  protected void setInputPath(String inputPath) {
    this.inputPath = inputPath;
  }

  int getPartitionNumber() {
    return partitionNumber;
  }

  protected void setPartitionNumber(int partitionNumber) {
    this.partitionNumber = partitionNumber;
  }

  protected void setAlignerExtraArgs(String alignerExtraArgs) {
    this.alignerExtraArgs = alignerExtraArgs;
  }

  protected String getAlignerExtraArgs() {
    return this.alignerExtraArgs;
  }

  void setOutputFile(String outputFile) {
    this.outputFile = outputFile;
  }

  protected String getOutputFile() {
    return this.outputFile;
  }

  abstract public ArrayList<String> generateParameters(int algorithmState, String inputFile1, String inputFile2);
}
