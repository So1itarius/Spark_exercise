import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
//import ru.itfb.education.bigdata.spark.model.UserAction;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class LogAnalyzerDriver {

    /*private static final SimpleDateFormat DATE_TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    private static final long TIME_INTERVAL = 120000L;//900000L;

    public static void main(String[] args) {
        //countUniqueUsers();
        //mapUsersWithIps();
        //mapIpWithUsers();
        findSuspiciousActions();
    }

    private static void countUniqueUsers() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setMaster("local").setAppName("Count unique users"));

        sparkContext
                .textFile("input/logs_example.csv")
                .map(line -> line.split(",")[4])
                .filter(login -> login != null && login.length() > 0)
                .distinct()
                .groupBy()
                .aggregateByKey()
                .sortBy(login -> login, true, 1)
                .saveAsTextFile("output/unique_users");

        sparkContext.stop();
    }

    private static void mapUsersWithIps() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setMaster("local").setAppName("Map users with ips"));

        sparkContext
                .textFile("input/logs_example.csv")
                .mapToPair(line -> {
                    String[] values = line.split(",");
                    return new Tuple2<>(values[4], values[2]);
                })
                .distinct()
                .filter(tuple -> tuple._1 != null && tuple._1.length() > 0)
                .reduceByKey((ip1, ip2) -> ip1 + "," + ip2)
                .sortByKey()
                .map(tuple -> tuple._1 + "\t" + tuple._2)
                .saveAsTextFile("output/user_ips");

        sparkContext.stop();
    }

    private static void mapIpWithUsers() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setMaster("local").setAppName("Map ip with users"));

        sparkContext
                .textFile("input/logs_example.csv")
                .mapToPair(line -> {
                    String[] values = line.split(",");
                    return new Tuple2<>(values[2], values[4]);
                })
                .distinct()
                .filter(tuple -> tuple._1 != null && tuple._1.length() > 0)
                .reduceByKey((ip1, ip2) -> ip1 + "," + ip2)
                .sortByKey()
                .map(tuple -> tuple._1 + "\t" + tuple._2)
                .saveAsTextFile("output/ip_users");

        sparkContext.stop();
    }

    private static void findSuspiciousActions() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setMaster("local").setAppName("Count unique users"));

        sparkContext
                .textFile("input/logs_example.csv")
                .map(line -> {
                    String[] values = line.split(",");
                    return UserAction
                            .builder()
                            .login(values[4])
                            .ip(values[2])
                            .type(values[3])
                            .time(DATE_TIME_FORMAT.parse(values[5]).getTime())
                            .build();
                })
                .filter(action -> "LOGIN".equals(action.getType()) && action.getLogin() != null && action.getLogin().length() > 0)
                .groupBy(UserAction::getIp)
                .aggregateByKey(
                        new ArrayList<Tuple2<UserAction, UserAction>>(),
                        (buffer, userActions) -> {
                            List<UserAction> actions = StreamSupport.stream(userActions.spliterator(), false).collect(Collectors.toList());
                            for (int i = 0; i < actions.size(); i++) {
                                UserAction current = actions.get(i);
                                for (int j = i + 1; j < actions.size(); j++) {
                                    UserAction next = actions.get(j);
                                    if (!current.getLogin().equals(next.getLogin()) && Math.abs(current.getTime() - next.getTime()) <= TIME_INTERVAL) {
                                        buffer.add(
                                                new Tuple2<>(current, next)
                                        );
                                    }
                                }
                            }

                            return buffer;
                        },
                        (buffer1, buffer2) -> {
                            buffer1.addAll(buffer2);
                            return buffer1;
                        }
                )
                .sortByKey()
                .flatMap(tuple3 ->
                    tuple3._2
                            .stream()
                            .map(tuple2 -> tuple3._1 + "," + tuple2._1.getLogin() + "," + new Date(tuple2._1.getTime()) + "," + tuple2._2.getLogin() + "," + new Date(tuple2._2.getTime()))
                            .collect(Collectors.toList())
                            .iterator()
                )
                .saveAsTextFile("output/suspicious_actions");

        sparkContext.stop();
    }*/

}
