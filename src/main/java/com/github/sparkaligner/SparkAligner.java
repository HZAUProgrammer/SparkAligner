package com.github.sparkaligner;

import com.github.sparkaligner.aligners.bwa.Bwa;

import java.util.Arrays;

public class SparkAligner {

  public static void main(String[] args) {
    if (args.length < 1) {
        System.err.println("At least one argument must be provided!");
      System.exit(-1);
    }

    String alignerName = args[0].toLowerCase();
    String argsNoAlignerName[] = Arrays.copyOfRange(args, 1, args.length);

    BaseAligner aligner = null;
    switch (alignerName) {
      case "bwa":
        aligner = new Bwa(argsNoAlignerName);
        break;
    }

    if (aligner != null) {
      aligner.run();
    } else {
      System.err.println(alignerName + " was not found!");
      System.exit(-2);
    }
  }
}
