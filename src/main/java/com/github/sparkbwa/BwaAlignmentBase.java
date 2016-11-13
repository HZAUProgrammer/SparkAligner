/**
 * Copyright 2016 José Manuel Abuín Mosquera <josemanuel.abuin@usc.es>
 *
 * <p>This file is part of SparkBWA.
 *
 * <p>SparkBWA is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * <p>SparkBWA is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * <p>You should have received a copy of the GNU General Public License along with SparkBWA. If not,
 * see <http://www.gnu.org/licenses/>.
 */
package com.github.sparkbwa;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

public abstract class BwaAlignmentBase implements Serializable {

  protected static final long serialVersionUID = 1L;
  /** < Version ID */
  protected static final Log LOG = LogFactory.getLog(BwaAlignmentBase.class);

  protected String appName = "";
  protected String appId = "";
  protected String tmpDir = "";
  protected Bwa bwaInterpreter;
  protected File originInputFile;

  /**
   * Constructor for this class
   *
   * @brief This constructor creates a BwaAlignment object to process in each one of the mappers
   * @param context the SparkContext to use
   * @param bwaInterpreter the Bwa object used to perform the alignment
   */
  public BwaAlignmentBase(SparkContext context, Bwa bwaInterpreter, File originInputFile) {

    this.appId = context.applicationId();
    this.appName = context.appName();
    this.tmpDir = context.getLocalProperty("spark.local.dir");
    this.bwaInterpreter = bwaInterpreter;
    this.originInputFile = originInputFile;

    //We set the tmp dir
    if (this.tmpDir == null || this.tmpDir == "null") {
      this.tmpDir = context.hadoopConfiguration().get("hadoop.tmp.dir");
    }

    if (this.tmpDir.startsWith("file:")) {
      this.tmpDir = this.tmpDir.replaceFirst("file:", "");
    }

    this.LOG.info("JMAbuin:: " + this.appId + " - " + this.appName);
  }

  public void alignReads(String outputSamFileName, String fastqFileName1, String fastqFileName2) {
    boolean isPairedReads = true;

    if (fastqFileName2 != null) {
      isPairedReads = false;
    }

    this.bwaInterpreter.setOutputFile(this.tmpDir + outputSamFileName);

    //We run BWA with the corresponding options set
    this.bwaInterpreter.run(0, fastqFileName1, fastqFileName2);

    //In case of the ALN algorithm, more executions of BWA are needed
    if (this.bwaInterpreter.isAlnAlgorithm()) {

      //The next execution of BWA in the case of ALN algorithm
      this.bwaInterpreter.run(1, fastqFileName1, fastqFileName2);

      //Finally, if we are talking about paired reads and aln algorithm, a final execution is needed
      if (isPairedReads) {
        this.bwaInterpreter.run(2, fastqFileName1, fastqFileName2);

        //Delete .sai file number 2
        File tmpSaiFile2 = new File(fastqFileName2 + ".sai");
        tmpSaiFile2.delete();
      }

      //Delete *.sai file number 1
      File tmpSaiFile1 = new File(fastqFileName1 + ".sai");
      tmpSaiFile1.delete();
    }
  }

  public ArrayList<String> copyResults(String outputSamFileName) {
    ArrayList< String> returnedValues = new ArrayList<String>();
    String baseOutputDir = this.originInputFile.getParent();
    File outputDir = new File(baseOutputDir, "sparkbwa-out-" + appId);

    if (!outputDir.exists()) {
      outputDir.mkdir();
    }

    String localOutputFile = this.bwaInterpreter.getOutputFile();
    this.LOG.info(localOutputFile);

    this.LOG.info("JMAbuin:: " + this.appId + " - " + this.appName + " Copying files...");
    try {
        if (outputDir.getPath().startsWith("hdfs")) {
          Configuration conf = new Configuration();
          FileSystem fs = FileSystem.get(conf);

          fs.copyFromLocalFile(
                  new Path(localOutputFile),
                  new Path(outputDir + "/" + outputSamFileName)
          );
      } else {
        File localSamOutput = new File(localOutputFile);
        Files.copy(Paths.get(localSamOutput.getPath()), Paths.get(outputDir.getPath(), localSamOutput.getName()));

      }
    } catch (IOException e) {
      e.printStackTrace();
      this.LOG.error(e.toString());
    }

    // Delete the old results file
    File tmpSamFullFile = new File(localOutputFile);
    tmpSamFullFile.delete();

    returnedValues.add(outputDir + "/" + outputSamFileName);

    return returnedValues;
  }

  public String getOutputSamFilename(Integer readBatchID) {
    return this.appName + "-" + this.appId + "-" + readBatchID + ".sam";
  }

  public ArrayList<String> runAlignmentProcess(
      Integer readBatchID, String fastqFileName1, String fastqFileName2) {
    //The output filename (without the tmp directory)
    String outputSamFileName = this.getOutputSamFilename(readBatchID);
    this.alignReads(outputSamFileName, fastqFileName1, fastqFileName2);

    // Copy the result to HDFS
    return this.copyResults(outputSamFileName);
  }
}
