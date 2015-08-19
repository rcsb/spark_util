package org.rcsb.spark.util;

import java.io.FileNotFoundException;
import java.io.PrintWriter;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

/**
 * Saves DataFrame to delimited files.
 * @author Peter Rose
 *
 */
public class DataFrameToDelimitedFileWriter {

    /**
     * Writes a Spark DataFrame to a comma separated value file
     * @param fileName
     * @param dataFrame
     * @throws FileNotFoundException
     */
    public static void writeCsv(String fileName, DataFrame dataFrame) throws FileNotFoundException {
        write(fileName, ",", dataFrame);
    }

    /**
     * Writes a Spark DataFrame to a tab separated value file
     * @param fileName
     * @param dataFrame
     * @throws FileNotFoundException
     */
    public static void writeTsv(String fileName, DataFrame dataFrame) throws FileNotFoundException {
        write(fileName, "\t", dataFrame);
    }

    /**
     * Writes a Spark DataFrame to delimited value file.
     * @param fileName
     * @param delimiter delimiter for data fields
     * @param dataFrame
     * @throws FileNotFoundException
     */
    public static void write(String fileName, String delimiter, DataFrame dataFrame) throws FileNotFoundException {
        PrintWriter writer = new PrintWriter(fileName);

        // write column headers
        String[] cols = dataFrame.columns();
        for (int i = 0; i < cols.length; i++) {
            writer.print(cols[i]);
            if (i < cols.length-1) {
                writer.print(delimiter);
            }
        }
        writer.println();

        // write delimited data
        for (Row r: dataFrame.collect()) {
            StringBuilder sb = new StringBuilder();

            for (int i = 0; i < r.length(); i++) {
                if (r.getAs(i) != null) {
                    sb.append(r.getAs(i).toString());
                }
                if (i < r.length()-1) {
                    sb.append(delimiter);
                }
            }
            writer.println(sb.toString());
        }
        writer.close();
    }
}