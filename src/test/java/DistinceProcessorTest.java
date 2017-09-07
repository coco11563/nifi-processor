//import org.junit.Test;
//
//import java.io.BufferedReader;
//import java.io.File;
//import java.io.IOException;
//import java.io.InputStreamReader;
//
//public class DistinceProcessorTest {
//    private String script;
//    @Test
//    public void test() {
//        String spark_path = "spark_path";
//        String master = "yarn";
//        String main_class = "main.class";
//        String deploy_mode = "client";
//        String executor_num = "1";
//        String driver_memory = "3";
//        String executor_memory = "10";
//        String total_executor_cores = "10";
//        String[] jar_path = new String[] {"1.jar","2.jar"};
//        String in = "lst";
//
//        String out = "drv";
//        String hive_server = "10.0.82.183";
//        StringBuilder sb = new StringBuilder();
//        sb.append(spark_path).append(File.separator)
//                .append("spark-submit ")
//                .append(" --master ").append(master)
//                .append(" --class ").append(main_class)
//                .append(" --deploy-mode ").append(deploy_mode)
//                .append(" --num-executors ").append(executor_num)
//                .append(" --driver-memory ").append(driver_memory).append("g")
//                .append(" --executor-memory ").append(executor_memory).append("g")
//                .append(" --total-executor-cores ").append(total_executor_cores)
//                .append(" --jars ");
//        for (String s : jar_path) {
//            sb.append(s).append(" ");
//        }
//        sb.append(" --driver-java-options " + "\"-Dhivein=").append(in).append(" -Dhiveout=").append(out).append(" -Dhiveserver=").append(hive_server).append(   "\"");
//        script = sb.toString();
//        System.out.println(sb.toString());
//    }
//    @Test
//    public void scriptRun() {
//        ProcessBuilder p = new ProcessBuilder();
//        p.command(script);
//        p.redirectErrorStream(true);
//        Process process = null;
//        try {
//            process = p.start();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        StringBuilder result = new StringBuilder();
//
//        final BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
//        try {
//            String line;
//            while ((line = reader.readLine()) != null) {
//                result.append(line);
//                System.out.println(p.command().toString() + " --->: " + line);
////                logger.info(p.command().toString() + " --->: " + line);
//            }
//        } catch (IOException e) {
//            System.out.println("failed to read output from process" );
//            e.printStackTrace();
////            logger.warn("failed to read output from process", e);
//        } finally {
//            try {
//                reader.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//
//        try {
//            process.waitFor();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//
//        int exit = process.exitValue();
//        if (exit != 0) {
//            try {
//                throw new IOException("failed to execute:" + p.command() + " with result:" + result);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//}
