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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Class that communicates with BWA
 *
 * @author José M. Abuín
 * @brief This class is where the actual communication with BWA is performed.
 */
public class Bwa implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Log LOG = LogFactory.getLog(Bwa.class);

  //Option to use the reduce phase
  private boolean useReducer = false; // The option to use the reduce phase

  //Algorithms boolean variables
  private boolean memAlgorithm = false; // The option to use the MEM algorithm
  private boolean alnAlgorithm = false; // The option to use the ALN algorithm
  private boolean bwaswAlgorithm = false; // The option to use the BWASW algorithm
  private String bwaArgs = ""; // The args passed directly to bwa

  //Index path
  private String indexPath = "";
  private String outputFile = "";
  private String outputHdfsDir = "";

  /**
   * @author José M. Abuín
   * @brief This constructor is used when the BWA options are already set
   * @param memAlgorithm The option to use the MEM algorithm
   * @param alnAlgorithm The option to use the ALN algorithm
   * @param bwaswAlgorithm The option to use the BWASW algorithm
   * @param pairedReads Use single reads
   * @param singleReads Use paired reads
   * @param indexPath The index path
   */
  public Bwa(BwaOptions options) {
    this.memAlgorithm = options.isMemAlgorithm();
    this.alnAlgorithm = options.isAlnAlgorithm();
    this.bwaswAlgorithm = options.isBwaswAlgorithm();

    this.bwaArgs = options.getBwaArgs();
    this.indexPath = options.getIndexPath();
    this.outputHdfsDir = options.getOutputPath();
  }

  /**
   * Getter for the output file
   *
   * @return A String containing the ouput SAM file name
   */
  public String getOutputFile() {
    return outputFile;
  }

  /**
   * Setter for the ouput file
   *
   * @param outputFile A String containing the output SAM file name
   */
  public void setOutputFile(String outputFile) {
    this.outputFile = outputFile;
  }


  /**
   * Getter for the option of using the mem algorithm or not
   *
   * @return A boolean value that is true if the mem algorithm is going to be used or false
   *     otherwise
   */
  public boolean isMemAlgorithm() {
    return memAlgorithm;
  }

  /**
   * Setter for the option of using the mem algorithm or not
   *
   * @param memAlgorithm A boolean value that is true if the mem algorithm is going to be used or
   *     false otherwise
   */
  public void setMemAlgorithm(boolean memAlgorithm) {
    this.memAlgorithm = memAlgorithm;
  }

  /**
   * Getter for the option of using the aln algorithm or not
   *
   * @return A boolean value that is true if the aln algorithm is going to be used or false
   *     otherwise
   */
  public boolean isAlnAlgorithm() {
    return alnAlgorithm;
  }

  /**
   * Setter for the option of using the aln algorithm or not
   *
   * @param alnAlgorithm A boolean value that is true if the aln algorithm is going to be used or
   *     false otherwise
   */
  public void setAlnAlgorithm(boolean alnAlgorithm) {
    this.alnAlgorithm = alnAlgorithm;
  }

  /**
   * Getter for the option of using the bwasw algorithm or not
   *
   * @return A boolean value that is true if the bwasw algorithm is going to be used or false
   *     otherwise
   */
  public boolean isBwaswAlgorithm() {
    return bwaswAlgorithm;
  }

  /**
   * Setter for the option of using the bwasw algorithm or not
   *
   * @param bwaswAlgorithm A boolean value that is true if the bwasw algorithm is going to be used
   *     or false otherwise
   */
  public void setBwaswAlgorithm(boolean bwaswAlgorithm) {
    this.bwaswAlgorithm = bwaswAlgorithm;
  }

  /**
   * Getter for the index path
   *
   * @return A String containing the index path
   */
  public String getIndexPath() {
    return indexPath;
  }

  /**
   * Setter for the index path
   *
   * @param indexPath A String that indicates the location of the index path
   */
  public void setIndexPath(String indexPath) {
    this.indexPath = indexPath;
  }

  /**
   * Getter for the output HDFS path
   *
   * @return A String containing the output HDFS path
   */
  public String getOutputHdfsDir() {
    return outputHdfsDir;
  }

  /**
   * Setter for the output HDFS path
   *
   * @param outputHdfsDir A String that indicates the output location for the SAM files in HDFS
   */
  public void setOutputHdfsDir(String outputHdfsDir) {
    this.outputHdfsDir = outputHdfsDir;
  }

  private String[] parseParameters(int alnStep, String inputFile1, String inputFile2) {
    ArrayList<String> parameters = new ArrayList<String>();

    //The first parameter is always "bwa"======================================================
    parameters.add("bwa");

    //The second parameter is the algorithm election===========================================
    String algorithm = "";

    boolean isPaired = true;
    if (inputFile2 == null) {
      isPaired = false;
    }

    //Case of "mem" algorithm
    if (this.memAlgorithm && !this.alnAlgorithm && !this.bwaswAlgorithm) {
      algorithm = "mem";
    }
    //Case of "aln" algorithm
    else if (!this.memAlgorithm && this.alnAlgorithm && !this.bwaswAlgorithm) {
      //Aln algorithm and paired reads
      if (isPaired) {
        //In the two first steps, the aln option is used
        if (alnStep < 2) {
          algorithm = "aln";
        }
        //In the third one, the "sampe" has to be performed
        else {
          algorithm = "sampe";
        }
      }
      //Aln algorithm single reads
      else if (!isPaired) {
        //In the first step the "aln" ins performed
        if (alnStep == 0) {
          algorithm = "aln";
        }
        //In the second step, the "samse" is performed
        else {
          algorithm = "samse";
        }
      }

    }
    //The last case is the "bwasw"
    else if (!this.memAlgorithm && !this.alnAlgorithm && this.bwaswAlgorithm) {
      algorithm = "bwasw";
    }

    parameters.add(algorithm);

    if (!this.bwaArgs.isEmpty()) {
      parameters.addAll(Arrays.asList(this.bwaArgs.split(" ")));
    }

    //The third parameter is the output file===================================================
    parameters.add("-f");

    if (algorithm.equals("aln")) {
      if (alnStep == 0) {
        parameters.add(inputFile1 + ".sai");
      } else if (alnStep == 1 && isPaired) {
        parameters.add(inputFile2 + ".sai");
      }
    } else {
      // For all other algorithms the output is a SAM file.
      parameters.add(this.outputFile);
    }

    //The fifth, the index path===============================================================
    parameters.add(indexPath);

    //The sixth, the input files===============================================================

    //If the "mem" algorithm, we add the FASTQ files
    if (algorithm.equals("mem") || algorithm.equals("bwasw")) {
      parameters.add(inputFile1);

      if (isPaired) {
        parameters.add(inputFile2);
      }
    }

    //If "aln" aln step is 0 or 1, also FASTQ files
    else if (algorithm.equals("aln")) {
      if (alnStep == 0) {
        parameters.add(inputFile1);
      } else if (alnStep == 1 && isPaired) {
        parameters.add(inputFile2);
      }
    }

    //If "sampe" the input files are the .sai from previous steps
    else if (algorithm.equals("sampe")) {
      parameters.add(inputFile1 + ".sai");
      parameters.add(inputFile2 + ".sai");
      parameters.add(inputFile1);
      parameters.add(inputFile2);
    }

    //If "samse", only one .sai file
    else if (algorithm.equals("samse")) {
      parameters.add(inputFile1 + ".sai");
      parameters.add(inputFile1);
    }

    String[] parametersArray = new String[parameters.size()];
    return parameters.toArray(parametersArray);
  }

  /**
   * This Function is responsible for creating the options that are going to be passed to BWA
   *
   * @param alnStep An integer that indicates at with phase of the aln step the program is
   * @return A Strings array containing the options which BWA was launched
   */
  public int run(int alnStep, String inputFile1, String inputFile2) {
    // Get obtain the list of arguments passed by the user
    String[] parametersArray = parseParameters(alnStep, inputFile1, inputFile2);

    // Call to JNI with the selected parameters
    int returnCode = BwaJni.Bwa_Jni(parametersArray);

    if (returnCode != 0) {
      LOG.error("BWA:: BWA exited with error code: " + String.valueOf(returnCode));
      return returnCode;
    }

    // The run was successful
    return 0;
  }
}
