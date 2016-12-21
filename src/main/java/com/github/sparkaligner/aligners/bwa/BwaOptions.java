package com.github.sparkaligner.aligners.bwa;

import com.github.sparkaligner.AlignerOptions;
import org.apache.commons.cli.*;

import java.util.ArrayList;
import java.util.Arrays;

public class BwaOptions extends AlignerOptions {

  public enum Algorithm {
    MEM, ALN, BWASW
  }

  private Algorithm algorithm = Algorithm.MEM;

  private String correctUse = "sparkaligner";

  public BwaOptions(String[] args) {
    Options options = this.initOptions();

    HelpFormatter formatter = new HelpFormatter();

    CommandLine cmd = this.parseArguments(options, args);

    if (cmd.hasOption("algorithm")) {
      String algorithmArg = cmd.getOptionValue("algorithm");

      if (algorithmArg.equals("mem")) {
        algorithm = Algorithm.MEM;
      } else if (algorithmArg.equals("aln")) {
        algorithm = Algorithm.ALN;
      } else if (cmd.getOptionValue("algorithm").equals("bwasw")) {
        algorithm = Algorithm.BWASW;
      } else {
        this.LOG.warn(
                "The algorithm "
                        + algorithmArg
                        + " could not be found\nSetting to default mem algorithm\n");
      }
    }

    if (cmd.hasOption("R")) {
      setIndexPath(cmd.getOptionValue("R"));
    }

    if (cmd.hasOption("partitions")) {
      setPartitionNumber(Integer.parseInt(cmd.getOptionValue("partitions")));
    }

    if (cmd.hasOption("bwaArgs")) {
      setAlignerExtraArgs(cmd.getOptionValue("bwaArgs"));
    }

    if (cmd.hasOption("I")) {
      setInputPath(cmd.getOptionValue("I"));
    }

  }

  public Options initOptions() {

    Options options = new Options();

    Option algorithm =
        new Option("algorithm", true, "Specify the algorithm to use during the alignment");
    algorithm.setArgName("mem|aln|bwasw");
    options.addOption(algorithm);

    Option bwaArgs = new Option("bwaArgs", true, "Arguments passed directly to BWA");
    bwaArgs.setArgName("\"BWA arguments\"");
    options.addOption(bwaArgs);

    Option index =
        new Option(
            "R",
            "reference",
             true,
             "Prefix for the index created by bwa to use - setIndexPath(string)");
    index.setArgName("Index prefix");
    options.addOption(index);

    Option partitions =
        new Option(
            "partitions",
            true,
            "Number of partitions to divide input reads - setPartitionNumber(int)");
    options.addOption(partitions);

    Option inputFolder =
            new Option(
                    "I",
                    "input",
                    true,
                    "The folder(s) containing the input files.");
    partitions.setArgName("Input folder");
    options.addOption(inputFolder);

    return options;
  }

  public ArrayList<String> generateParameters(int alnStep, String inputFile1, String inputFile2) {
    ArrayList<String> parameters = new ArrayList<>();

    String algorithm;

    boolean isPaired = true;
    if (inputFile2 == null) {
      isPaired = false;
    }

    if (isMemAlgorithm()) {
      algorithm = "mem";
    }
    else if (isAlnAlgorithm()) {
      if (isPaired) {
        if (alnStep == 2) {
          algorithm = "sampe";
        } else {
          algorithm = "aln";
        }
      }  else {
        // In the first step the "aln" ins performed
        if (alnStep == 2) {
          algorithm = "samse";
        } else {
          algorithm = "aln";
        }
      }
    } else {
      algorithm = "bwasw";
    }

    parameters.add(algorithm);

    if (!this.getAlignerExtraArgs().isEmpty()) {
      parameters.addAll(Arrays.asList(this.getAlignerExtraArgs().split(" ")));
    }

    if (isAlnAlgorithm()) {
      parameters.add("-f");
      if (alnStep == 0) {
        parameters.add(inputFile1 + ".sai");
      } else if (alnStep == 1 && isPaired) {
        parameters.add(inputFile2 + ".sai");
      }
    }

    parameters.add(this.getIndexPath());

    if (isMemAlgorithm() || isBwaswAlgorithm()) {
      parameters.add(inputFile1);

      if (isPaired) {
        parameters.add(inputFile2);
      }
    }
    else if (isAlnAlgorithm()) {
      if (alnStep == 0) {
        parameters.add(inputFile1);
      } else if (alnStep == 1 && isPaired) {
        parameters.add(inputFile2);
      }
    }
    else if (algorithm.equals("sampe")) {
      parameters.add(inputFile1 + ".sai");
      parameters.add(inputFile2 + ".sai");
      parameters.add(inputFile1);
      parameters.add(inputFile2);
    }
    else if (algorithm.equals("samse")) {
      parameters.add(inputFile1 + ".sai");
      parameters.add(inputFile1);
    }

    if (!isAlnAlgorithm()) {
      parameters.add(">");
      parameters.add(this.getOutputFile());
    }

    return parameters;
  }

  public boolean isMemAlgorithm() {
    return algorithm == Algorithm.MEM;
  }

  public void setMemAlgorithm() {
      this.algorithm = Algorithm.MEM;
  }

  public boolean isAlnAlgorithm() {
    return algorithm == Algorithm.ALN;
  }

  public void setAlnAlgorithm() {
    this.algorithm = Algorithm.ALN;
  }

  public boolean isBwaswAlgorithm() {
    return algorithm == Algorithm.BWASW;
  }

  public void setBwaswAlgorithm() {
    this.algorithm = Algorithm.BWASW;
  }
}
