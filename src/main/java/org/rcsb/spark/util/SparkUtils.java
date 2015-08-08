package org.rcsb.spark.util;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by andreas on 8/7/15.
 */
public class SparkUtils {


    public static SparkConf getSimpleConfig() {

        return getSimpleConfig("Simple Application");
    }

    public static SparkConf getSimpleConfig(String appName){
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        int cores = Runtime.getRuntime().availableProcessors();
        System.out.println("SparkUtils: Available cores: " + cores);
        SparkConf conf = new SparkConf()
                .setMaster("local[" + cores + "]")
                .setAppName(appName)
                .set("spark.driver.maxResultSize", "4g")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryoserializer.buffer.max", "1g");

        return conf;
    }

    public static SparkContext getSparkContext() {

        SparkConf conf = getSimpleConfig();

        SparkContext sc = new SparkContext(conf);

        return sc;
    }

    public static SparkContext getSparkContext(String appName) {

        SparkConf conf = getSimpleConfig(appName);

        SparkContext sc = new SparkContext(conf);

        return sc;
    }

    public static JavaSparkContext getJavaSparkContext() {

        SparkConf conf = getSimpleConfig();

        JavaSparkContext sc = new JavaSparkContext(conf);

        return sc;
    }


    public static JavaSparkContext getJavaSparkContext(String appName) {

        SparkConf conf = getSimpleConfig(appName);

        JavaSparkContext sc = new JavaSparkContext(conf);

        return sc;
    }

    public static SQLContext getSqlContext(SparkContext sc) {

        SQLContext sqlContext = new SQLContext(sc);

        sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy");

        sqlContext.setConf("spark.sql.parquet.filterPushdown", "true");

        return sqlContext;
    }

    public static SQLContext getSqlContext(JavaSparkContext sc) {

        SQLContext sqlContext = new SQLContext(sc);

        sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy");

        sqlContext.setConf("spark.sql.parquet.filterPushdown", "true");

        return sqlContext;
    }


    /** renames all Columns in a DataFrame according to RCSB conventions.
     * This is to ensure that all columns are named according to our spec for data-sharing with other users.
     *
     * @param original
     * @return
     */
    public static DataFrame toRcsbConvention(DataFrame original)  {

        for (String existingName : original.columns()){

            String newName = toRcsbConvention(existingName);

            original = original.withColumnRenamed(existingName,newName);

        }

        return original;

    }


    private static Properties props = null;
    private  static String propFileName = "/rcsb.mapping.properties";

    private static Properties getProperties()  {
        if  (props == null){
            props = new Properties();
            InputStream inStream = SparkUtils.class.getResourceAsStream(propFileName);

            if (inStream != null){
                try {
                    props.load(inStream);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else
                System.err.println("properties file " + propFileName + " not found on classpath!");
        }
        return props;
    }


    /** Consults the rcsb.mapping.properties file for a naming convention
     * If no convention is found, converts
     * PascalCase and camelCase to
     * pascal_case and camel_case
     *
     * @param column
     * @return
     * @throws IOException
     */
    private static String toRcsbConvention(String column)  {


        boolean hasMixedCase = hasMixedCase(column);

        if ( ! hasMixedCase ){
            column = column.toLowerCase();
        } // mixed case means camelCase or PascalCase

        Properties props = getProperties();

        String newName = props.getProperty(column);

        if ( newName != null)
            return newName;

        if ( ! hasMixedCase)
            return column;

        // mixed cases need to be dealt with
        // specially:

        StringBuffer b = new StringBuffer();
        for ( int i = 0 ; i< column.length() ; i++){
            char ch = column.charAt(i);

            if ( Character.isUpperCase(ch) ) {
                // should not be the case.

                if ( i > 0) {
                    // camel case
                    // we need to split here
                    b.append("_");

                } else {
                    // i == 0 ... PascalCase, automatically dealt with
                }
                b.append(Character.toLowerCase(ch));
            } else {
                b.append(ch);
            }

        }

        return b.toString();


    }

    private static boolean hasMixedCase(String column) {

        if ( column == null)
            return false;
        if ( column.length() == 0)
            return false;



        Boolean firstCase = null;
        for (char c : column.toCharArray()) {
            if ( ! Character.isAlphabetic(c))
                continue;

            if ( firstCase == null) {
                firstCase = Character.isUpperCase(c);
                continue;
            }

            if ( ! firstCase == Character.isUpperCase(c))
                return true;
        }
        return false;
    }
}
