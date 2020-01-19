import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class SparkSQL {
    private static void total_consumption(SparkSession spark) {
        Dataset<Row> dataset = spark
                .read()
                .format("csv")
                .option("header", "true")
                .load("input/energy-usage-2010.csv");

        dataset.createOrReplaceTempView("Energy");
        spark.sql("SET spark.sql.parser.quotedRegexColumnNames=true");
        Dataset<Row> sqlDF = spark.sql("SELECT `COMMUNITY AREA NAME`," +
                                                     //" `KWH\\s(?!MEAN|MINIMUM|MAXIMUM)[A-Z]+\\s2010`" +
                                                     " SUM(`KWH JANUARY 2010`)," +
                                                     " SUM(`KWH FEBRUARY 2010`)," +
                                                     " SUM(`KWH MARCH 2010`)" +
                                                     //и т.д.
                                                    " FROM Energy" +
                                               " GROUP BY `COMMUNITY AREA NAME`"
        );
        //sqlDF.show();
        sqlDF.repartition(1).write().format("csv").option("header", "true").save("output/task_1");
    }

    private static void most_popular_videos (SparkSession spark) {
        Dataset<Row> dataset = spark
                .read()
                .format("csv")
                .option("header", "true")
                .load("input/RUvideos.csv");

        dataset.createOrReplaceTempView("VideoList");
        Dataset<Row> sqlDF = spark.sql("SELECT title, " +
                                                      "CAST(views as int) AS v" +
                                               " FROM VideoList" +
                                               " ORDER BY cast(views as int) DESC");
        sqlDF.limit(10).show();

    }
    private static void most_popular_videos_for_month (SparkSession spark) {
        Dataset<Row> dataset = spark
                .read()
                .format("csv")
                .option("header", "true")
                .load("input/RUvideos.csv");
        dataset.createOrReplaceTempView("VideoList");

        Dataset<Row> sqlDF = spark.sql( "SELECT title," +
                                                      " V," +
                                                      " Date" +
                                               " FROM (select title, " +
                                                             "cast(views as int) AS V, " +
                                                             "concat_ws('.',split(trending_date,'[\\.]')[2],split(trending_date,'[\\.]')[0]) as Date, " +
                                                             "row_number() over(PARTITION BY CONCAT_WS('.',split(trending_date,'[\\.]')[2],split(trending_date,'[\\.]')[0]) order by cast(views as int) desc) num" +
                                                     " from VideoList" +
                                                     " order by date desc,v desc)" +
                                               " WHERE num = 1");
        //sqlDF2.show();
        sqlDF.repartition(1).write().format("csv").option("header", "true").save("output/task_3");
    }
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        Path output = new Path("output");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL")
                .config("spark.master", "local")
                .getOrCreate();

        //total_consumption(spark);
        //most_popular_videos(spark);
        //most_popular_videos_for_month(spark);
        spark.stop();
    }
}

