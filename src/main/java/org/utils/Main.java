package org.utils;

import org.apache.log4j.Logger;
import org.sparkoperations.SomeSparkOperation;
import org.sparkoperations.SparkInit;

public class Main {
    static Logger log = Logger.getLogger(Main.class);

    public static void main(String[] args) {
        System.out.println("Hello world!");
        SparkInit.init();
        SomeSparkOperation sparkOp = new SomeSparkOperation();
        sparkOp.practiceForEach(SparkInit.sc);
        System.out.println("Hello Completed!");
     for(long i =1L;i<=9000L;i++)
        {
            log.info("From the logger.........counter:"+i);
            log.info("It is a long established fact that a reader will be distracted by the readable content of a page when looking at its layout. The point of using Lorem Ipsum is that it has a more-or-less normal distribution of letters, as opposed to using 'Content here, content here', making it look like readable English. Many desktop publishing packages and web page editors now use Lorem Ipsum as their default model text, and a search for 'lorem ipsum' will uncover many web sites still in their infancy. Various versions have evolved over the years, sometimes by accident, sometimes on purpose (injected humour and the like).");
        }

        //System.exit(-1);
        //org.apache.log4j.LogManager.shutdown();
    }
}