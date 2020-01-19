import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class SparkSQLTasks {
    private static void AVG_time(SparkSession spark,Dataset<Row> dataset) {

        //dataset.printSchema();
        dataset.createOrReplaceTempView("company");
        //spark.sql("SET spark.sql.parser.quotedRegexColumnNames=true");
        Dataset<Row> sqlDF = spark.sql("SELECT insurer_nm," +
                                                     " AVG(s) AS avg " +
                                               "FROM (select insurer_nm," +
                                                            " claim_id," +
                                                            " sum(total_hrs_in_status) as s " +
                                                     "from company " +
                                                     "group by insurer_nm, claim_id) " +
                                               "GROUP BY insurer_nm");
        //sqlDF.show();
        sqlDF.repartition(1).write().format("csv").option("header", "true").save("output/task_4");
    }

    private static void course_users(SparkSession spark,Dataset<Row> dataset) {


        //dataset1.printSchema();
        dataset.createOrReplaceTempView("event_data");
        Dataset<Row> sqlDF = spark.sql(" SELECT user_id AS ev_us_id" + //, count(action) as c_a" +
                                               " FROM event_data" +
                                               " WHERE action=\"passed\"" +
                                               " GROUP BY user_id" +
                                               " HAVING COUNT(action)=(select count(distinct step_id) from event_data)");
        //sqlDF1.show();
        sqlDF.repartition(1).write().format("csv").option("header", "true").save("output/task_5");
    }

    private static void completed_tasks(SparkSession spark,Dataset<Row> dataset_1,Dataset<Row> dataset_2) {

        dataset_1.createOrReplaceTempView("event_data");
        Dataset<Row> sqlDF_1 = spark.sql("SELECT user_id as ev_us_id" + //, count(action) as c_a" +
                                                " FROM event_data" +
                                                " WHERE action=\"passed\"" +
                                                " GROUP BY user_id" +
                                                " HAVING COUNT(action)=(select count(distinct step_id) from event_data)");

        //dataset1.printSchema();
        dataset_2.createOrReplaceTempView("submissions_data");
        Dataset<Row> sqlDF_2 = spark.sql(" SELECT user_id AS s_us_id," +
                                                 " COUNT(step_id) AS c_st" +
                                                 " FROM submissions_data" +
                                                 " WHERE submission_status=\"correct\" " +
                                                 " GROUP BY user_id");
        //sqlDF2.show();
        //sqlDF2.repartition(1).write().format("csv").option("header", "true").save("output/result");

        Dataset<Row> joined = sqlDF_1.join(sqlDF_2, sqlDF_1.col("ev_us_id").equalTo(sqlDF_2.col("s_us_id")), "inner");
        joined.createOrReplaceTempView("all_info");
        Dataset<Row> sqlDF_3 = spark.sql("SELECT * FROM all_info ORDER BY c_st DESC");
        //sqlDF3.show();

        sqlDF_3.repartition(1).write().format("csv").option("header", "true").save("output/task_6");
    }
    private static void most_difficult_task(SparkSession spark,Dataset<Row> dataset) {
        dataset.createOrReplaceTempView("submissions_data");
        Dataset<Row> sqlDF4_1 = spark.sql("SELECT user_id," +
                                                        " step_id" +
                                                 " FROM (select user_id," +
                                                              " step_id," +
                                                              " timestamp," +
                                                              " submission_status," +
                                                              " row_number() over(PARTITION BY user_id order by user_id, timestamp desc) num" +
                                                       " from submissions_data" +
                                                       " order by user_id, timestamp)" +
                                                " WHERE num = 1");
        Dataset<Row> sqlDF4_2 = spark.sql("SELECT user_id," +
                                                        " step_id" +
                                                 " FROM submissions_data" +
                                                 " WHERE submission_status = \"correct\"");
        //sqlDF4.show();
        //sqlDF4_1.repartition(1).write().format("csv").option("header", "true").save("output/result/2");

        //Dataset<Row> sqlDF5 = spark.sql(select
        sqlDF4_1.except(sqlDF4_2).createOrReplaceTempView("result");
        sqlDF4_1.except(sqlDF4_2).repartition(1).write().format("csv").option("header", "true").save("output/task_8");
        Dataset<Row> sqlDF4_3 = spark.sql("SELECT step_id," +
                                                        " COUNT(step_id) AS c" +
                                                 " FROM result" +
                                                 " GROUP BY step_id" +
                                                 " ORDER BY COUNT(step_id) DESC" +
                                                 " LIMIT 1");
        sqlDF4_3.show();
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

        Dataset<Row> insurance_ds = spark
                .read()
                .format("csv")
                .option("header", "true")
                .load("input/data-1542534337679.csv");

        Dataset<Row> event_ds = spark
                .read()
                .format("csv")
                .option("header", "true")
                .load("input/event_data_train.csv");

        Dataset<Row> submissions_ds = spark
                .read()
                .format("csv")
                .option("header", "true")
                .load("input/submissions_data_train.csv");

        //AVG_time(spark,insurance_ds);
        //course_users(spark,event_ds);
        //completed_tasks(spark,event_ds, submissions_ds);
        //most_difficult_task(spark,submissions_ds);
        spark.stop();
    }
}
