package pub.sha0w.nifi.processors;


import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * you need to provide input hive table
 * and output hive table
 * then this processor will find the duplicate stuff
 * and delete them
 * then using sparkSQL to insert into hive table
 */
@Tags({"Spark","Distinct","shell"})
@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
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
            .description("使用空格分割，输入绝对路径，先输入spark依赖包，再输入spark包")
            .build();

    //input hive table name
    private final static PropertyDescriptor INPUT_HIVE_TABLE = new PropertyDescriptor.Builder()
            .name("input")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .description("这部分输入的是需要输入的Hive表名，会使用SparkSQL进行读取，这部分会以-D的形式作为运行参数")
            .required(true)
            .defaultValue("")
            .build();
    //output hive table name
    private final static PropertyDescriptor OUTPUT_HIVE_TABLE = new PropertyDescriptor.Builder()
            .name("output")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .description("这部分输入的是需要输出的Hive表名，会使用SparkSQL进行读取，这部分会以-D的形式作为运行参数")
            .required(true)
            .defaultValue("")
            .build();
    //hive server ip
    private final static PropertyDescriptor HIVE_SERVER = new PropertyDescriptor.Builder()
            .name("hive server")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .description("hive的地址，这部分会以-D的形式作为运行参数")
            .required(true)
            .defaultValue("packone168")
            .build();
    private final static PropertyDescriptor MAIN_ARGS = new PropertyDescriptor.Builder()
            .name("MianArgs")
            .description("附加给JVM的参数，会加-D执行")
            .required(true)
            .build();
    private final static PropertyDescriptor VAGUR_ARG = new PropertyDescriptor.Builder()
            .name("VagueArgs")
            .description("附加给JVM的参数，会加-D执行")
            .required(true)
            .build();
    private final static PropertyDescriptor FK_ARG = new PropertyDescriptor.Builder()
            .name("FKArgs")
            .description("附加给JVM的参数，会加-D执行，需要记录的外键")
            .required(true)
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
        //run bash
        Process process = null;
        try {
            process = submitApplications(processContext);
            process.waitFor();
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }

    private Process submitApplications(final ProcessContext context)
            throws InterruptedException, IOException {
        final Map<PropertyDescriptor, String> env = context.getProperties();
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
        String sparkSubmitCMD =
                getPropertyValue(SPARK_PATH, context) + "/bin/spark-submit";
        String mK = getPropertyValue(MAIN_ARGS, context);
        String fK = getPropertyValue(FK_ARG, context);
        String vK = getPropertyValue(VAGUR_ARG, context);
        final String out = env.get(OUTPUT_HIVE_TABLE);
        final String in = env.get(INPUT_HIVE_TABLE);
        final String hive_server = env.get(HIVE_SERVER);
        String[] cmdArray = {
                sparkSubmitCMD,
                "--class",
                getPropertyValue(MAIN_CLASS, context),
                "--master",
                master,
                "--deploy-mode",
                deploy_mode,
                "--num-executors",
                getPropertyValue(EXECUTOR_NUM, context),
                "--driver-memory",
                getPropertyValue(DRIVER_MEMORY, context),
                "--executor-memory",
                getPropertyValue(DRIVER_MEMORY, context),
                "--total-executor-cores",
                getPropertyValue(TOTAL_EXECUTOR_CORES, context),
                "--jars",
                getPropertyValue(JAR_PATH, context),
                "--driver-java-options",
                "\"-Dhivein=" + in +
                        " -Dhiveserver=" + hive_server +
                        " -Dhiveout=" + out +
                        " -Dmk=" + mK +
                        " -Dfk=" + fK +
                        " -Dvk=" + vK +
                        "\""
        };
        List<String> cmdList = new ArrayList<>();
        cmdList.addAll(Arrays.asList(cmdArray));
        ProcessBuilder pb = new ProcessBuilder(cmdList);

        pb.redirectErrorStream(true);
        Process sparkSubmit = pb.start();

        sparkSubmit.waitFor();

        InputStreamReader isr = new InputStreamReader(sparkSubmit.getInputStream());
        BufferedReader br = new BufferedReader(isr);

        String lineRead;
        while ((lineRead = br.readLine()) != null) {
            logger.debug(lineRead);
        }

        return sparkSubmit;
    }
    private String getPropertyValue(PropertyDescriptor property, final ProcessContext context) {
        return context.getProperty(property).getValue();
    }
}
