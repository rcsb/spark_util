package org.rcsb.spark.util;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * Created by andreas on 8/7/15.
 */
public class SparkUtils {


    private static SparkContext getSparkContext() {
        return getSparkContext("Simple Application");
    }

    private static SparkContext getSparkContext(String appName) {


        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        int cores = Runtime.getRuntime().availableProcessors();
        System.out.println("Available cores: " + cores);
        SparkConf conf = new SparkConf()
                .setMaster("local[" + cores + "]")
                .setAppName(appName)
                .set("spark.driver.maxResultSize", "4g")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryoserializer.buffer.max", "1g");

        SparkContext sc = new SparkContext(conf);

        return sc;
    }

    private static SQLContext getSqlContext(SparkContext sc) {

        SQLContext sqlContext = new SQLContext(sc);

        sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy");

        sqlContext.setConf("spark.sql.parquet.filterPushdown", "true");

        return sqlContext;
    }

    private static SQLContext getSqlContext(JavaSparkContext sc) {

        SQLContext sqlContext = new SQLContext(sc);

        sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy");

        sqlContext.setConf("spark.sql.parquet.filterPushdown", "true");

        return sqlContext;
    }
}
