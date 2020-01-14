import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class SparkSQL {
    public static void main(String[] args) throws IOException, AnalysisException {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL")
                .config("spark.master", "local")
                .getOrCreate();

        runBasicDataFrameExample(spark);
        spark.stop();
    }

    private static void runBasicDataFrameExample(SparkSession spark) throws AnalysisException {
        Dataset<Row> dataset = spark
                .read()
                .format("csv")
                .option("header", "true")
                .load("input/energy-usage-2010.csv");

        //dataset.printSchema();
        dataset.createOrReplaceTempView("Energy");
        Dataset<Row> sqlDF = spark.sql("SELECT `COMMUNITY AREA NAME`," +
                                                       "SUM(`KWH JANUARY 2010`)," +
                                                       "SUM(`KWH FEBRUARY 2010`)," +
                                                       "SUM(`KWH MARCH 2010`)" +
                "                                      FROM Energy GROUP BY `COMMUNITY AREA NAME`");
        //sqlDF.show();
        //sqlDF.repartition(1).write().format("csv").option("header", "true").save("output/result");
        Dataset<Row> dataset1 = spark
                .read()
                .format("csv")
                .option("header", "true")
                .load("input/RUvideos.csv");
        //dataset1.printSchema();
        dataset1.createOrReplaceTempView("VideoList");
        Dataset<Row> sqlDF1 = spark.sql("SELECT title, cast(views as int) " +
                                                        "FROM VideoList ORDER BY cast(views as int) desc");
        //sqlDF1.show();
        Dataset<Row> sqlDF2 = spark.sql("SELECT first(title)," +
                                                   " max(cast(views as int)) AS V," +
                                                   " CONCAT_WS('.',split(trending_date,'[\\.]')[2],split(trending_date,'[\\.]')[0]) as Date" +
                                                   " FROM VideoList" +
                                                   " group by date"+
                                                   " Order by v DESC");
        sqlDF2.show();
        //sqlDF2.repartition(1).write().format("csv").option("header", "true").save("output/result");

    }
}

