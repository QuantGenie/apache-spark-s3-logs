package org.sparkoperations;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Iterator;

public class SomeSparkOperation {



    public static void practiceForEach(JavaSparkContext sc)
    {
        RDDSource source = new RDDSource(sc);
        JavaRDD<Integer> input = source.getIntegerRDD(1,2,3).repartition(10);

        Accumulator<Integer> partition = sc.accumulator(0, "partitionCounter");

        input.foreachPartition(new VoidFunction<Iterator<Integer>>() {
            @Override
            public void call(Iterator<Integer> integerIterator) throws Exception {
                partition.add(1);
                while (integerIterator.hasNext())
                {
                    System.out.println("int value: "+integerIterator.next()+"counter: "+partition);
                }
            }
        });

        System.out.println("Total partitions calculated manually are..."+partition);


        JavaRDD<String> strings = source.getStringRDD("string1", "string2", "string3");

        strings.foreachPartition(new VoidFunction<Iterator<String>>() {
            @Override
            public void call(Iterator<String> stringIterator) throws Exception {
                while (stringIterator.hasNext())
                {
                    System.out.println("String value: "+stringIterator.next());
                }
            }
        });

    }

}
