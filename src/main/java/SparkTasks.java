import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class SparkTasks {
    public static void main(String[] args) throws IOException, AnalysisException {
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

        runBasicDataFrameExample(spark);
        spark.stop();
    }

    private static void runBasicDataFrameExample(SparkSession spark) throws AnalysisException {
        Dataset<Row> dataset = spark
                .read()
                .format("csv")
                .option("header", "true")
                .load("input/data-1542534337679.csv");

        //dataset.printSchema();
        dataset.createOrReplaceTempView("company");
        //spark.sql("SET spark.sql.parser.quotedRegexColumnNames=true");
        Dataset<Row> sqlDF = spark.sql("SELECT insurer_nm, avg(s) as avg from (SELECT insurer_nm, claim_id, sum(total_hrs_in_status) as s" +
                                               " FROM company" +
                                               " GROUP BY insurer_nm,claim_id) group by insurer_nm");
        //sqlDF.show();
        //sqlDF.repartition(1).write().format("csv").option("header", "true").save("output/result");

        Dataset<Row> dataset1 = spark
                .read()
                .format("csv")
                .option("header", "true")
                .load("input/event_data_train.csv");
        //dataset1.printSchema();
        dataset1.createOrReplaceTempView("event_data");
        Dataset<Row> sqlDF1 = spark.sql("select user_id as ev_us_id, count(action) as c_a" +
                                                " from event_data" +
                                                " where action=\"passed\"" +
                                                " Group by user_id" +
                                                " HAVING count(action)=(select count(distinct step_id) from event_data)");
        //sqlDF1.show();
        //sqlDF1.repartition(1).write().format("csv").option("header", "true").save("output/result");

        Dataset<Row> dataset2 = spark
                .read()
                .format("csv")
                .option("header", "true")
                .load("input/submissions_data_train.csv");
        //dataset1.printSchema();
        dataset2.createOrReplaceTempView("submissions_data");
        Dataset<Row> sqlDF2 = spark.sql("select user_id as s_us_id, count(step_id) as c_st from submissions_data where submission_status=\"correct\" group by user_id");
        //sqlDF2.show();
        //sqlDF2.repartition(1).write().format("csv").option("header", "true").save("output/result");

        Dataset <Row> joined = sqlDF1.join(sqlDF2, sqlDF1.col("ev_us_id").equalTo(sqlDF2.col("s_us_id")),"inner");
        joined.createOrReplaceTempView("all_info");
        Dataset<Row> sqlDF3 = spark.sql("SELECT * from all_info order by c_st desc");
        //sqlDF3.show();

        sqlDF3.repartition(1).write().format("csv").option("header", "true").save("output/result");

    }

}
