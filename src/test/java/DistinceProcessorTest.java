import org.junit.Test;

import java.io.File;

public class DistinceProcessorTest {
    @Test
    public void test() {
        String spark_path = "spark_path";
        String master = "yarn";
        String main_class = "main.class";
        String deploy_mode = "client";
        String executor_num = "1";
        String driver_memory = "3";
        String executor_memory = "10";
        String total_executor_cores = "10";
        String[] jar_path = new String[] {"1.jar","2.jar"};
        String in = "lst";

        String out = "drv";
        String hive_server = "10.0.82.183";
        StringBuilder sb = new StringBuilder();
        sb.append(spark_path).append(File.separator)
                .append("spark-submit ")
                .append(" --master ").append(master)
                .append(" --class ").append(main_class)
                .append(" --deploy-mode ").append(deploy_mode)
                .append(" --num-executors ").append(executor_num)
                .append(" --driver-memory ").append(driver_memory).append("g")
                .append(" --executor-memory ").append(executor_memory).append("g")
                .append(" --total-executor-cores ").append(total_executor_cores)
                .append(" --jars ");
        for (String s : jar_path) {
            sb.append(s).append(" ");
        }
        sb.append(" --driver-java-options " + "\"-Dhivein=").append(in).append(" -Dhiveout=").append(out).append(" -Dhiveserver=").append(hive_server).append(   "\"");

        System.out.println(sb.toString());
    }
}
