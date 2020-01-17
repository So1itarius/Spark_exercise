import lombok.val;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class SparkDriver {
    protected static String Converter(String s) throws ParseException {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.ms");
        //Date date = null;
        //try { date = dateFormat.parse(s.toString().split(" ",2)[1]); } catch (ParseException e) { e.printStackTrace(); }
        Date date = dateFormat.parse(s);
        long unixTime = (Objects.requireNonNull(date)).getTime() /1000;
        return String.valueOf(unixTime);

    }


    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        Path output = new Path("output");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }

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


        /*JavaSparkContext sparkContext2 = new JavaSparkContext(
                new SparkConf()
                        .setAppName("Word Count").setMaster("local")
        );

        sparkContext2
                .textFile("input/logs_example.csv")
                .map(s-> s.split(","))
                //.collect(Collectors.groupingBy(p->p.eq))
                //.collect(Collectors.toList())s
                .mapToPair(word->new Tuple2<String,String>(word[4], word[2]))
                //.reduceByKey((a1,a2)->a1+a2)
                //.reduceByKey((a1,a2)->a1+a2)
                .distinct()
                .groupByKey()
                //.collect()
                .saveAsTextFile("output/pair");
        sparkContext2.stop();*/

        /*JavaSparkContext sparkContext3 = new JavaSparkContext(
                new SparkConf()
                        .setAppName("Word Count").setMaster("local")
        );

        sparkContext3
                .textFile("input/logs_example.csv")
                .map(s-> s.split(","))
                //.collect(Collectors.groupingBy(p->p.eq))
                //.collect(Collectors.toList())s
                .mapToPair(word->new Tuple2<String,String>(word[2], word[4]))
                //.reduceByKey((a1,a2)->a1+a2)
                //.reduceByKey((a1,a2)->a1+a2)
                .distinct()
                .groupByKey()
                //.collect()
                .saveAsTextFile("output/pair1");
        sparkContext3.stop();*/

        JavaSparkContext sparkContext4 = new JavaSparkContext(
                new SparkConf()
                        .setAppName("Word Count").setMaster("local")
        );


        sparkContext4
                .textFile("input/logs_example.csv")
                .map(s-> s.split(","))
                .filter(anObject -> "LOGIN".equals(anObject[3]));
                //.collect(Collectors.groupingBy(p->p.eq))
                //.collect(Collectors.toList())s
                //.mapToPair(line->new Tuple2<String,ArrayList<String>>(line[2],line[4], Converter(line[6])))
                //.mapToPair(line->new Tuple3<String,String,String>(line[2], line[4], Converter(line[6])))
                //.reduceByKey((a1,a2)->a1+a2)
                //.reduceByKey((a1,a2)->a1+a2)
                //.distinct()
                //.groupByKey()
                //.mapValues(s.groupByKey())
                //.countByValue()
                //.sorted()
                //.mapToPair()
                //.collect(Collectors.groupingBy((p) -> p[0]))
                //.foreach(x->System.out.println(x));
                //.forEach(x -> System.out.println(x));
                //.sorted(Comparator.naturalOrder())
                //.saveAsTextFile("output/spamUser");

        sparkContext4.stop();

        //System.out.println(sparkContext4));

    }
}
