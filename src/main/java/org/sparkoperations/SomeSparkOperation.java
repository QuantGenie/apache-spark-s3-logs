package org.sparkoperations;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Iterator;

public class SomeSparkOperation {



    public static void practiceForEach(JavaSparkContext sc)
    {
        RDDSource source = new RDDSource(sc);
        JavaRDD<Integer> input = source.getIntegerRDD(1,2,3).repartition(10);

    }

}
