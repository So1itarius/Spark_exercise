import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WordCountDriver {
    public static void main(String[] args){
        /*JavaSparkContext sparkContext = new JavaSparkContext(
                new SparkConf()
                .setAppName("Word Count").setMaster("local")
        );

        sparkContext
                .textFile("input/doc1.txt")
                .flatMap(s-> Stream.of(s.split(" ")).iterator())
                .mapToPair(word->new Tuple2<>(word, 1))
                .reduceByKey((a1,a2)->a1+a2)
                .saveAsTextFile("output/wordcount");
        sparkContext.stop();*/

        /*JavaSparkContext sparkContext1 = new JavaSparkContext(
                new SparkConf()
                        .setAppName("Word Count").setMaster("local")
        );

        sparkContext1
                .textFile("input/logs_example.csv")
                .flatMap(s-> Stream.of(s.split(",")[4]).iterator())
                .distinct()
                //.mapToPair(word->new Tuple2<>(word, 1))
                //.reduceByKey((a1,a2)->a1+a2)
                .saveAsTextFile("output/logs");
        sparkContext1.stop();*/


        JavaSparkContext sparkContext2 = new JavaSparkContext(
                new SparkConf()
                        .setAppName("Word Count").setMaster("local")
        );

        sparkContext2
                .textFile("input/logs_example.csv")
                .flatMap(s-> Stream.of(s.split(",")[4]+' '+s.split(",")[2]).iterator())
                //.collect(Collectors.groupingBy(p->p.eq))
                //.collect(Collectors.toList())s
                .mapToPair(word->new Tuple2<String,String>(word.split(" ")[0], word.split(" ")[1]))
                //.reduceByKey((a1,a2)->a1+a2)
                //.reduceByKey((a1,a2)->a1+a2)
                .groupByKey()
                //.collect()
                .saveAsTextFile("output/pair");
        sparkContext2.stop();


    }
}
