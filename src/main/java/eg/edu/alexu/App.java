package eg.edu.alexu;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class App {
    public static void main( String[] args ) {
        final String inputfile = args[0];
        JavaSparkContext spark = new JavaSparkContext("local[*]", "BigData-Lab2"); //local machine all cores (*)
        try {
            JavaRDD<String> logFile = spark.textFile(inputfile);
            System.out.printf("Number of lines in the log file %d\n", logFile.count());
        } finally {
            spark.close();
        }
    }
}

//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import scala.Tuple2;
//
//import java.io.IOException;
//import java.util.Arrays;
//
//public class App
//{
//    public static void main( String[] args ) throws IOException {
//        JavaSparkContext sc = new JavaSparkContext("local", "test");
//        JavaRDD<String> lines = sc.textFile("wordcount-input.txt");
//        JavaRDD<String> words = lines.flatMap(l -> Arrays.asList(l.split("\\b")).iterator());
//        JavaPairRDD<String, Integer> wordPairs = words.mapToPair(w -> new Tuple2<>(w, 1));
//        JavaPairRDD<String, Integer> wordCounts = wordPairs.reduceByKey((a,b) -> a + b);
//        wordCounts.saveAsTextFile("wordcount-output.txt");
//        System.in.read();
//        sc.stop();
//    }
//}


