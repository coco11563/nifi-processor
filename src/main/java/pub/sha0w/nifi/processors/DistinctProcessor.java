package pub.sha0w.nifi.processors;


import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.logging.Logger;

/**
 * you need to provide input hive table
 * and output hive table
 * then this processor will find the duplicate stuff
 * and delete them
 * then using sparkSQL to insert into hive table
 */
public class DistinctProcessor extends AbstractProcessor {
    private final org.slf4j.Logger logger = LoggerFactory.getLogger(DistinctProcessor.class);
    //main class
    private static final PropertyDescriptor MAIN_CLASS = new PropertyDescriptor.Builder()
            .name("main class")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue("Main")
            .required(true)
            .build();
    //master
    private static final PropertyDescriptor MASTER = new PropertyDescriptor.Builder()
            .name("master")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue("yarn")
            .required(true)
            .allowableValues(new String[] {"yarn", "mesos", "standalone"})
            .build();
    //deploy mode
    private static final PropertyDescriptor DEPLOY_MODE = new PropertyDescriptor.Builder()
            .name("deploy mode")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue("client")
            .allowableValues(new String[] {"client", "cluster"})
            .build();
    //driver memory
    private static final PropertyDescriptor DRIVER_MEMORY = new PropertyDescriptor.Builder()
            .name("driver memory/g")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .required(true)
            .defaultValue("8")
            .build();
    //executor memory
    private static final PropertyDescriptor EXECUTOR_MEMORY = new PropertyDescriptor.Builder()
            .name("executor memory/g")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .required(true)
            .defaultValue("4")
            .build();
    //num executor
    private static final PropertyDescriptor EXECUTOR_NUM = new PropertyDescriptor.Builder()
            .name("num executor")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .required(true)
            .defaultValue("8")
            .build();
    //total executor cores
    private static final PropertyDescriptor TOTAL_EXECUTOR_CORES = new PropertyDescriptor.Builder()
            .name("total executor cores")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .required(true)
            .defaultValue("32")
            .build();
    //spark path
    private static final PropertyDescriptor SPARK_PATH = new PropertyDescriptor.Builder()
            .name("spark home")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .defaultValue("/usr/hdp/2.6.1.0-129/spark")
            .build();
    //SSjars
    private static final PropertyDescriptor JAR_PATH = new PropertyDescriptor.Builder()
            .name("jar path")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .defaultValue("/usr/hdp/2.6.1.0-129/spark/bin/")
            .build();

    //input hive table name
    private final static PropertyDescriptor INPUT_HIVE_TABLE = new PropertyDescriptor.Builder()
            .name("input")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .defaultValue("")
            .build();
    //output hive table name
    private final static PropertyDescriptor OUTPUT_HIVE_TABLE = new PropertyDescriptor.Builder()
            .name("output")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .defaultValue("")
            .build();
    //hive server ip
    private final static PropertyDescriptor HIVE_SERVER = new PropertyDescriptor.Builder()
            .name("hive server")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .defaultValue("packone168")
            .build();
    // ip + port
    private static final PropertyDescriptor SPARK_IP = new PropertyDescriptor.Builder()
            .name("spark id")
            .defaultValue("")
            .build();
    private final static List<PropertyDescriptor> propertyDescriptors;
    private final static Set<Relationship> relationships;

    private static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("sucess")
            .build();

    private static final Relationship REL_RERUN = new Relationship.Builder()
            .name("re-run")
            .build();


    static {
        List<org.apache.nifi.components.PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(MAIN_CLASS);
        _propertyDescriptors.add(MASTER);
        _propertyDescriptors.add(DEPLOY_MODE);
        _propertyDescriptors.add(SPARK_IP);
        _propertyDescriptors.add(DRIVER_MEMORY);
        _propertyDescriptors.add(EXECUTOR_MEMORY);
        _propertyDescriptors.add(EXECUTOR_NUM);
        _propertyDescriptors.add(TOTAL_EXECUTOR_CORES);
        _propertyDescriptors.add(SPARK_PATH);
        _propertyDescriptors.add(JAR_PATH);
        _propertyDescriptors.add(OUTPUT_HIVE_TABLE);
        _propertyDescriptors.add(HIVE_SERVER);
        _propertyDescriptors.add(INPUT_HIVE_TABLE);

        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);
        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_RERUN);

        relationships = Collections.unmodifiableSet(_relationships);
    }


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }


    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {
        final Map<PropertyDescriptor, String> env = processContext.getProperties();
        final String master;
        final String deploy_mode;
        if (Objects.equals(env.get(MASTER), "yarn")) {
            master = "yarn";
            deploy_mode = env.get(DEPLOY_MODE);
        } else if(Objects.equals(env.get(MASTER),"mesos")) {
            master = "mesos://" + env.get(SPARK_IP);
            deploy_mode = env.get(DEPLOY_MODE);
        }else {
            master = "spark://" + env.get(SPARK_IP);
            deploy_mode = env.get(DEPLOY_MODE);
        }
        final String main_class = env.get(MAIN_CLASS);
        final String spark_path = env.get(SPARK_PATH);
        final String[] jar_path = env.get(JAR_PATH).split(",");


        //initial spark runtime config
        final String driver_memory = env.get(DRIVER_MEMORY);
        final String executor_memory = env.get(EXECUTOR_MEMORY);
        final String executor_num = env.get(EXECUTOR_NUM);
        final String total_executor_cores = env.get(TOTAL_EXECUTOR_CORES);
        //initial hive config
        final String out = env.get(OUTPUT_HIVE_TABLE);
        final String in = env.get(INPUT_HIVE_TABLE);
        final String hive_server = env.get(HIVE_SERVER);
        //generate spark submit script
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
        String script = sb.toString();
        ProcessBuilder p = new ProcessBuilder();
        p.command(script);
        p.redirectErrorStream(true);
        Process process = null;
        try {
            process = p.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
        StringBuilder result = new StringBuilder();
        final BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        try {
            String line;
            while ((line = reader.readLine()) != null) {
                result.append(line);
                logger.info(p.command().toString() + " --->: " + line);
            }
        } catch (IOException e) {
            logger.warn("failed to read output from process", e);
        } finally {
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            process.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        int exit = process.exitValue();
        if (exit != 0) {
            try {
                throw new IOException("failed to execute:" + p.command() + " with result:" + result);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
