package com.github.sparkaligner;

import java.io.*;
import java.util.ArrayList;

public class MiscUtils {

    public static String extractExecutable(String resourceFilePath) {

        if (!resourceFilePath.startsWith("/")) {
            resourceFilePath = "/" + resourceFilePath;
        }

        if (resourceFilePath == null)
            return null;

        try {
            // Read the file we're looking for
            InputStream fileStream = MiscUtils.class.getResourceAsStream(resourceFilePath);

            // Was the resource found?
            if (fileStream == null)
                return null;

            // Grab the file name
            String[] chopped = resourceFilePath.split("\\/");
            String fileName = chopped[chopped.length-1];

            // Create our temp file
            File tempFile = File.createTempFile("SparkAligner" + System.nanoTime(), fileName);

            // Delete the file on VM exit
            tempFile.deleteOnExit();

            OutputStream out = new FileOutputStream(tempFile);

            // Write the file to the temp file
            byte[] buffer = new byte[1024];
            int len = fileStream.read(buffer);
            while (len != -1) {
                out.write(buffer, 0, len);
                len = fileStream.read(buffer);
            }

            fileStream.close();
            out.close();

            tempFile.setExecutable(true);
            return tempFile.getAbsolutePath();

        } catch (IOException e) {
            return null;
        }
    }

    public static int executeResourceBinary(String binaryName, ArrayList<String> arguments) {
        String pathToUnpackedBinary = extractExecutable(binaryName);

        if (pathToUnpackedBinary == null) {
            System.err.println("Could not find binary: " + binaryName);
            return -1;
        }

        arguments.add(0, pathToUnpackedBinary);

        int numTries = 0;
        int maxRetries = 5;

        ProcessBuilder builder = new ProcessBuilder(arguments);
        if (arguments.get(arguments.size()-2).equals(">")) {
            String outputFile = arguments.get(arguments.size()-1);
            builder.redirectOutput(new File(outputFile));

            arguments.remove(arguments.size()-2);
            arguments.remove(arguments.size()-1);
        }

        Process p;
        while (true) {
            try {
                p = builder.start();
                p.waitFor();

                if (p.exitValue() != 0) {
                    System.err.println(binaryName + " exited with error code: " + p.exitValue());
                    InputStream errorStream = p.getErrorStream();
                    BufferedReader errorStreamReader = new BufferedReader(new InputStreamReader(errorStream));

                    String currLine = null;
                    while ((currLine = errorStreamReader.readLine()) != null) {
                        System.out.println(currLine);
                    }

                    return p.exitValue();
                }
                break;
            } catch (IOException e) {
                e.printStackTrace();
                if (++numTries == maxRetries) return -2;
            } catch (InterruptedException e) {
                e.printStackTrace();
                if (++numTries == maxRetries) return -3;
            }
        }

        return 0;
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

}
