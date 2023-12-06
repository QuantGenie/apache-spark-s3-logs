package org.sparkoperations;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class RDDSource {

    JavaSparkContext sc = null;

    public RDDSource(JavaSparkContext sc)
    {
        this.sc = sc;
    }

    public JavaRDD<String> getStringRDD(String ... stringValues)
    {
        return sc.parallelize((stringValues.length > 0)?
                Arrays.asList(stringValues) :
                Arrays.asList("value1", "value2", "value3"));
    }

    public JavaRDD<Integer> getIntegerRDD(Integer ... intValues)
    {
        return sc.parallelize((intValues.length > 0)?
                Arrays.asList(intValues) :
                Arrays.asList(1,2,3));
    }




}
