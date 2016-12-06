package com.github.sparkaligner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public abstract class BaseAligner implements Serializable {

    private static final Log LOG = LogFactory.getLog(BaseAligner.class);
    private static JavaSparkContext ctx;
    private String alignerName;

    protected AlignerOptions options;

    public BaseAligner(String alignerName, AlignerOptions options) {
        this.alignerName = alignerName;
        this.options = options;

        SparkConf sparkConf;
        if (this.ctx == null) {
            String appName = "SparkAligner-" + options.getPartitionNumber();

            sparkConf = new SparkConf().setAppName(appName);
            this.ctx = new JavaSparkContext(sparkConf);
        }
    }

    public BaseAligner(JavaSparkContext sparkContext, String alignerName, AlignerOptions options) {
        this.alignerName = alignerName;
        this.options = options;

        this.ctx = sparkContext;
    }

    abstract public void alignReads(String fastqFileName1, String fastqFileName2);

    protected int runAligner(ArrayList<String> args) {
        return MiscUtils.executeResourceBinary(alignerName, args);
    }

    public void run() {
        List<File> inputFiles = MiscUtils.getFilesInFolder(this.options.getInputPath());
        List<Tuple2<File, File>> pairedInputFiles = pairFastqFiles(inputFiles);

        List<String> returnedValues = new ArrayList<>();
        for (Tuple2<File, File> inputFileTuple : pairedInputFiles) {
            File inputFile1 = inputFileTuple._1;
            File inputFile2 = inputFileTuple._2;

            if (inputFile1 != null && inputFile2 != null) {
                JavaRDD<Tuple2<String, String>> readsRDD = handlePairedReadsSorting(inputFile1, inputFile2);
                returnedValues.addAll(mapPaired(this, readsRDD, inputFile1));
            } else {
                JavaRDD<String> readsRDD = handleSingleReadsSorting(inputFile1);
                returnedValues.addAll(mapSingle(this, readsRDD, inputFile1));
            }
        }
    }

    protected List<String> mapPaired(BaseAligner aligner, JavaRDD<Tuple2<String, String>> readsRDD, File inputFile) {
        return readsRDD
                .mapPartitionsWithIndex(new PairedAlignment(readsRDD.context(), aligner, inputFile), true)
                .collect();
    }

    protected List<String> mapSingle(BaseAligner aligner, JavaRDD<String> readsRDD, File inputFile) {
        return readsRDD
                .mapPartitionsWithIndex(new SingleAlignment(readsRDD.context(), aligner, inputFile), true)
                .collect();
    }


    public static List<Tuple2<File, File>> pairFastqFiles(List<File> inputFastqFiles) {
        HashMap<String, Tuple2<File, File>> fastqMapper = new HashMap<>();

        for (File currFastqFile : inputFastqFiles) {
            String fastqWithoutExt = MiscUtils.removeExtenstion(currFastqFile.getPath());
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

    protected JavaRDD<String> handleSingleReadsSorting(File inputFastq) {
        JavaPairRDD<Long, String> singleReadsKeyVal = loadFastq(this.ctx, inputFastq);

        return singleReadsKeyVal.repartition(options.getPartitionNumber()).values();
    }

    protected JavaRDD<Tuple2<String, String>> handlePairedReadsSorting(File inputFastq1, File inputFastq2) {
        JavaPairRDD<Long, String> datasetTmp1 = loadFastq(this.ctx, inputFastq1);
        JavaPairRDD<Long, String> datasetTmp2 = loadFastq(this.ctx, inputFastq2);

        JavaPairRDD<Long, Tuple2<String, String>> pairedReadsRDD = datasetTmp1.join(datasetTmp2);

        return pairedReadsRDD.repartition(options.getPartitionNumber()).values();
    }

    public int execute(int algorithmState, String inputFile1, String inputFile2) {
        ArrayList<String> parametersArray = this.options.generateParameters(algorithmState, inputFile1, inputFile2);
        int returnCode = this.runAligner(parametersArray);

        if (returnCode != 0) {
            LOG.error(this.alignerName + "exited with error code: " + String.valueOf(returnCode));
            return returnCode;
        }

        return 0;
    }
}
