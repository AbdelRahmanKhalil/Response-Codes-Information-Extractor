package eg.edu.alexu;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Filter {

    public static void main( String[] args ) {
        final String desiredCode = "200";
        final String inputfile = args[0];
        JavaSparkContext spark = new JavaSparkContext("local[*]", "BigData-Lab2"); //local machine all cores (*)
        try {
            JavaRDD<String> logFile = spark.textFile(inputfile);
            JavaRDD<String> matchingLines = logFile.filter(line -> line.split("\t")[5].equals(desiredCode));
            System.out.printf("The file '%s' contains %d lines with response code %s\n", inputfile, matchingLines.count(), desiredCode);

//            System.out.printf("Number of lines in the log file %d\n", logFile.count());
        } finally {
            spark.close();
        }
    }
}