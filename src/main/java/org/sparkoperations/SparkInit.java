package org.sparkoperations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkInit {

    //learn foreachAsync
    public static JavaSparkContext sc= null;
    public static void init()
    {
        System.out.println("Started RDD practicing...");
        String appName = "RDD practice";
        String master = "local";
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        sc = new JavaSparkContext(conf);

    }

}
