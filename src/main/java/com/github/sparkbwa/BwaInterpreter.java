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
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.ContextCleaner;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * BwaInterpreter class
 *
 * @author José M. Abuín
 * @brief This class communicates Spark with BWA
 */
public class BwaInterpreter {

  private static final Log LOG = LogFactory.getLog(BwaInterpreter.class);
  private SparkConf sparkConf;
  private JavaSparkContext ctx;
  private Configuration conf;
  private long totalInputLength;
  private long blocksize;
  private BwaOptions options;
  private String inputTmpFileName;

  /**
   * Constructor to build the BwaInterpreter object from the Spark shell When creating a
   * BwaInterpreter object from the Spark shell, the BwaOptions and the Spark Context objects need
   * to be passed as argument.
   *
   * @param opcions The BwaOptions object initialized with the user options
   * @param context The Spark Context from the Spark Shell. Usually "sc"
   * @return The BwaInterpreter object with its options initialized.
   */
  public BwaInterpreter(BwaOptions opcions, SparkContext context) {

    this.options = opcions;
    this.ctx = new JavaSparkContext(context);
    this.initInterpreter();
  }

  /**
   * Constructor to build the BwaInterpreter object from within SparkBWA
   *
   * @param args Arguments got from Linux console when launching SparkBWA with Spark
   * @return The BwaInterpreter object with its options initialized.
   */
  public BwaInterpreter(String[] args) {

    this.options = new BwaOptions(args);
    this.initInterpreter();
  }

  private void setTotalInputLength() {
    try {
      FileSystem fs = FileSystem.get(this.conf);

      // To get the input files sizes
      ContentSummary cSummaryFile1 = fs.getContentSummary(new Path(options.getInputPath()));

      long lengthFile1 = cSummaryFile1.getLength();
      long lengthFile2 = 0;

      // Total size. Depends on paired or single reads
      this.totalInputLength = lengthFile1 + lengthFile2;
      fs.close();
    } catch (IOException e) {
      LOG.error(e.toString());
      e.printStackTrace();
    }
  }

  private void createOutputFolder() {
    try {
      FileSystem fs = FileSystem.get(this.conf);

      // Path variable
      Path outputDir = new Path(options.getOutputPath());

      // Directory creation
      if (!fs.exists(outputDir)) {
        fs.mkdirs(outputDir);
      } else {
        fs.delete(outputDir, true);
        fs.mkdirs(outputDir);
      }

      fs.close();
    } catch (IOException e) {
      LOG.error(e.toString());
      e.printStackTrace();
    }
  }

  private JavaRDD<String> handleSingleReadsSorting(File inputFastq) {
    JavaRDD<String> readsRDD = null;

    long startTime = System.nanoTime();

    LOG.info("JMAbuin::Not sorting in HDFS. Timing: " + startTime);

    // Read the two FASTQ files from HDFS using the FastqInputFormat class
    JavaPairRDD<Long, String> singleReadsKeyVal = loadFastq(this.ctx, inputFastq);

    // Sort in memory with no partitioning
    if ((options.getPartitionNumber() == 0) && (options.isSortFastqReads())) {
      // First, the join operation is performed. After that,
      // a sortByKey. The resulting values are obtained
      readsRDD = singleReadsKeyVal.sortByKey().values();
      LOG.info("JMAbuin:: Sorting in memory without partitioning");
    }

    // Sort in memory with partitioning
    else if ((options.getPartitionNumber() != 0) && (options.isSortFastqReads())) {
      singleReadsKeyVal = singleReadsKeyVal.repartition(options.getPartitionNumber());
      readsRDD = singleReadsKeyVal.sortByKey().values().persist(StorageLevel.MEMORY_ONLY());
      LOG.info("JMAbuin:: Repartition with sort");
    }

    // No Sort with no partitioning
    else if ((options.getPartitionNumber() == 0) && (!options.isSortFastqReads())) {
      LOG.info("JMAbuin:: No sort and no partitioning");
      readsRDD = singleReadsKeyVal.values();
    }

    // No Sort with partitioning
    else {
      LOG.info("JMAbuin:: No sort with partitioning");
      int numPartitions = singleReadsKeyVal.partitions().size();

      /*
       * As in previous cases, the coalesce operation is not suitable
       * if we want to achieve the maximum speedup, so, repartition
       * is used.
       */
      if ((numPartitions) <= options.getPartitionNumber()) {
        LOG.info("JMAbuin:: Repartition with no sort");
      } else {
        LOG.info("JMAbuin:: Repartition(Coalesce) with no sort");
      }

      readsRDD =
          singleReadsKeyVal
              .repartition(options.getPartitionNumber())
              .values()
              .persist(StorageLevel.MEMORY_ONLY());
      long endTime = System.nanoTime();

      LOG.info("JMAbuin:: End of sorting. Timing: " + endTime);
      LOG.info("JMAbuin:: Total time: " + (endTime - startTime) / 1e9 / 60.0 + " minutes");
    }

    readsRDD.persist(StorageLevel.MEMORY_ONLY());

    return readsRDD;
  }

  public static ArrayList<File> getFilesInFolder(String pathToFolder) {
    File folder = new File(pathToFolder);
    File[] listOfFiles = folder.listFiles();

    if (listOfFiles != null) {
      ArrayList<File> fastqFiles = new ArrayList<>();

      for (File file : listOfFiles) {
        String fileName = file.getName().toLowerCase();
        if (file.isDirectory() && !fileName.startsWith("sparkbwa")) {
          ArrayList<File> filesInDir = getFilesInFolder(file.getPath());
          if (filesInDir != null) {
            fastqFiles.addAll(filesInDir);
          }
        }
        else if (fileName.endsWith("fastq") || fileName.endsWith("fq")) {
          fastqFiles.add(file);
        }
      }
      return fastqFiles;
    }

    System.err.println(String.format("The folder: %s is either empty or was not found!", folder.getName()));
    return null;
  }

  public static String removeExtenstion(String filepath) {
    String ext = "fastq";
    if (filepath.endsWith("fq")) {
      ext = "fq";
    }

    return filepath.substring(0, filepath.length() - (ext.length()+1));
  }

  public static List<Tuple2<File, File>> pairFastqFiles(List<File> inputFastqFiles) {
    HashMap<String, Tuple2<File, File>> fastqMapper = new HashMap<>();

    for (File currFastqFile : inputFastqFiles) {
      String fastqWithoutExt = removeExtenstion(currFastqFile.getPath());
      String pairName = fastqWithoutExt.substring(0, fastqWithoutExt.length()-3);
      Tuple2<File, File> newFastqPair;

      if (fastqMapper.containsKey(pairName)) {
        Tuple2<File, File> oldFastqPair = fastqMapper.get(pairName);
        newFastqPair = new Tuple2<>(oldFastqPair._1, currFastqFile);

      } else {
        newFastqPair = new Tuple2<>(currFastqFile, null);
      }

      fastqMapper.put(pairName, newFastqPair);
    }

    return new ArrayList<>(fastqMapper.values());
  }

  public static JavaPairRDD<Long, String> loadFastq(JavaSparkContext ctx, File inputFastqFiles) {
    JavaRDD<String> fastqLines = ctx.textFile(inputFastqFiles.getPath());

    // Determine which FASTQ record the line belongs to.
    JavaPairRDD<Long, Tuple2<String, Long>> fastqLinesByRecordNum = fastqLines.zipWithIndex().mapToPair(new FASTQRecordGrouper());

    // Group group the lines which belongs to the same record, and concatinate them into a record.
    return fastqLinesByRecordNum.groupByKey().mapValues(new FASTQRecordCreator());
  }

  private JavaRDD<Tuple2<String, String>> handlePairedReadsSorting(File inputFastq1, File inputFastq2) {
    JavaRDD<Tuple2<String, String>> readsRDD = null;

    long startTime = System.nanoTime();

    LOG.info("JMAbuin::Not sorting in HDFS. Timing: " + startTime);

    // Read the two FASTQ files from HDFS using the FastqInputFormat class
    JavaPairRDD<Long, String> datasetTmp1 = loadFastq(this.ctx, inputFastq1);
    JavaPairRDD<Long, String> datasetTmp2 = loadFastq(this.ctx, inputFastq2);
    JavaPairRDD<Long, Tuple2<String, String>> pairedReadsRDD = datasetTmp1.join(datasetTmp2);

    datasetTmp1.unpersist();
    datasetTmp2.unpersist();

    // Sort in memory with no partitioning
    if ((options.getPartitionNumber() == 0) && (options.isSortFastqReads())) {
      // First, the join operation is performed. After that,
      // a sortByKey. The resulting values are obtained
      readsRDD = pairedReadsRDD.sortByKey().values();
      LOG.info("JMAbuin:: Sorting in memory without partitioning");
    }

    // Sort in memory with partitioning
    else if ((options.getPartitionNumber() != 0) && (options.isSortFastqReads())) {
      pairedReadsRDD = pairedReadsRDD.repartition(options.getPartitionNumber());
      readsRDD = pairedReadsRDD.sortByKey().values().persist(StorageLevel.MEMORY_ONLY());
      LOG.info("JMAbuin:: Repartition with sort");
    }

    // No Sort with no partitioning
    else if ((options.getPartitionNumber() == 0) && (!options.isSortFastqReads())) {
      LOG.info("JMAbuin:: No sort and no partitioning");
    }

    // No Sort with partitioning
    else {
      LOG.info("JMAbuin:: No sort with partitioning");
      int numPartitions = pairedReadsRDD.partitions().size();

      /*
       * As in previous cases, the coalesce operation is not suitable
       * if we want to achieve the maximum speedup, so, repartition
       * is used.
       */
      if ((numPartitions) <= options.getPartitionNumber()) {
        LOG.info("JMAbuin:: Repartition with no sort");
      } else {
        LOG.info("JMAbuin:: Repartition(Coalesce) with no sort");
      }

      readsRDD =
          pairedReadsRDD
              .repartition(options.getPartitionNumber())
              .values()
              .persist(StorageLevel.MEMORY_ONLY());
    }

    long endTime = System.nanoTime();

    LOG.info("JMAbuin:: End of sorting. Timing: " + endTime);
    LOG.info("JMAbuin:: Total time: " + (endTime - startTime) / 1e9 / 60.0 + " minutes");
    readsRDD.persist(StorageLevel.MEMORY_ONLY());

    return readsRDD;
  }

  /**
   * Procedure to perform the alignment
   *
   * @author José M. Abuín
   */
  private List<String> MapPairedBwa(Bwa bwa, JavaRDD<Tuple2<String, String>> readsRDD, File inputFile) {
    // The mapPartitionsWithIndex is used over this RDD to perform the alignment. The resulting sam filenames are returned
    return readsRDD
        .mapPartitionsWithIndex(new BwaPairedAlignment(readsRDD.context(), bwa, inputFile), true)
        .collect();
  }

  private List<String> MapSingleBwa(Bwa bwa, JavaRDD<String> readsRDD, File inputFile) {
    // The mapPartitionsWithIndex is used over this RDD to perform the alignment. The resulting sam filenames are returned
    return readsRDD
        .mapPartitionsWithIndex(new BwaSingleAlignment(readsRDD.context(), bwa, inputFile), true)
        .collect();
  }

  /**
   * Runs BWA with the specified options
   *
   * @brief This function runs BWA with the input data selected and with the options also selected
   *     by the user.
   */
  public void runBwa() {
    LOG.info("JMAbuin:: Starting BWA");
    Bwa bwa = new Bwa(this.options);

    List<File> inputFiles = getFilesInFolder(this.options.getInputPath());
    List<Tuple2<File, File>> pairedInputFiles = pairFastqFiles(inputFiles);

    List<String> returnedValues = new ArrayList<>();
    for (Tuple2<File, File> inputFileTuple : pairedInputFiles) {
      File inputFile1 = inputFileTuple._1;
      File inputFile2 = inputFileTuple._2;
      if (inputFile1 != null && inputFile2 != null) {
        JavaRDD<Tuple2<String, String>> readsRDD = handlePairedReadsSorting(inputFile1, inputFile2);
        returnedValues.addAll(MapPairedBwa(bwa, readsRDD, inputFile1));
      } else {
        JavaRDD<String> readsRDD = handleSingleReadsSorting(inputFile1);
        returnedValues.addAll(MapSingleBwa(bwa, readsRDD, inputFile1));
      }
    }
  }

  /**
   * Procedure to init the BwaInterpreter configuration parameters
   *
   * @author José M. Abuín
   */
  public void initInterpreter() {
    //If ctx is null, this procedure is being called from the Linux console with Spark
    if (this.ctx == null) {

      String sorting;

      //Check for the options to perform the sort reads
      if (options.isSortFastqReads()) {
        sorting = "SortSpark";
      } else if (options.isSortFastqReadsHdfs()) {
        sorting = "SortHDFS";
      } else {
        sorting = "NoSort";
      }
      //The application name is set
      this.sparkConf =
          new SparkConf()
              .setAppName(
                  "SparkBWA_"
                      + options.getInputPath().split("/")[
                          options.getInputPath().split("/").length - 1]
                      + "-"
                      + options.getPartitionNumber()
                      + "-"
                      + sorting);
      //The ctx is created from scratch
      this.ctx = new JavaSparkContext(this.sparkConf);

    }
    //Otherwise, the procedure is being called from the Spark shell
    else {

      this.sparkConf = this.ctx.getConf();
    }
    //The Hadoop configuration is obtained
    this.conf = this.ctx.hadoopConfiguration();

    //The block size
    this.blocksize = this.conf.getLong("dfs.blocksize", 134217728);

    createOutputFolder();
    setTotalInputLength();

    ContextCleaner cleaner = this.ctx.sc().cleaner().get();
  }
}
