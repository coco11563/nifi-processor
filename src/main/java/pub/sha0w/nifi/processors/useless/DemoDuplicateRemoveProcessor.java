package pub.sha0w.nifi.processors.useless;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.pattern.PartialFunctions;
import org.apache.nifi.processor.util.pattern.RollbackOnFailure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pub.sha0w.nifi.processors.model.DuliModel;


import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

public class DemoDuplicateRemoveProcessor extends AbstractProcessor {
    /**
     * {@link Validator} that ensures that value contain ";" && after split the number of args must lower than 2
     */
    private static final Validator RULE_VALID_VALIDATOR = (subject, value, context) -> new ValidationResult.Builder().subject(subject).input(value).valid(value.contains(";") && value.split(";").length <= 2).explanation(subject + " must contain a \";\"").build();

    private final Logger logger = LoggerFactory.getLogger(DemoDuplicateRemoveProcessor.class);
    // Relationships

    //单纯需要插入的REL
    private static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully trans FlowFile")
            .build();
    //需要进行完善的REL
    private static final Relationship REL_COMPLETE = new Relationship.Builder()
            .name("complete")
            .description("this stuff is duplicate and maybe the record in db needs to be completed")
            .build();

    //PropertiesDescriptor
    //预设的去重模型参数，默认值为NONE，当为NONE时会去从FIELD中获取模型参数
    public static final PropertyDescriptor MODEL = new PropertyDescriptor.Builder()
            .name("model")
            .description("由一系列预设置的去重字段组成，如果为“NONE”时则调用“FIELD”中输入的由逗号分隔的字符串作为去重关键字")
            .required(true)
            .defaultValue("NONE")
            .allowableValues(MODEL_ENUM.values())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();  //PropertiesDescriptor

    //用户自定义的去重模型参数
    public static final PropertyDescriptor FIELD = new PropertyDescriptor.Builder()
            .name("field")
            .description("若MODEL中的数据为“NONE”则会从这个输入的字符串中获取所需要的参数。输入的参数" +
                    "的详细格式规则：条件内容用“，”分隔，详细条件与模糊条件按照“；”来进行分隔，与和或以“&”和“|”进行标识")
            .addValidator(RULE_VALID_VALIDATOR)
            .required(false)
            .build();
    //一个用以连接中间库的ControllerService
    private static final PropertyDescriptor CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("JDBC Connection Pool")
            .description("Specifies the JDBC Connection Pool to use in order to convert the JSON message to a SQL statement. "
                    + "The Connection Pool is necessary in order to determine the appropriate database column types.")
            .identifiesControllerService(DBCPService.class)
            .required(true)
            .build();

    private final static List<PropertyDescriptor> propertyDescriptors;
    private final static Set<Relationship> relationships;

    static {
        List<org.apache.nifi.components.PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(MODEL);
        _propertyDescriptors.add(FIELD);
        _propertyDescriptors.add(CONNECTION_POOL);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);
        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_COMPLETE);
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
    //PutSQL中获取连接的方法
    private final PartialFunctions.InitConnection<FunctionContext, Connection> initConnection = (c, s, fc) -> {
        final Connection connection = c.getProperty(CONNECTION_POOL).asControllerService(DBCPService.class).getConnection();
        try {
            fc.originalAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new ProcessException("Failed to disable auto commit due to " + e, e);
        }
        return connection;
    };
    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {
        //init process way
        final DBCPService dbcp = processContext.getProperty(DemoDuplicateRemoveProcessor.CONNECTION_POOL).asControllerService(DBCPService.class);
        MODEL_ENUM model_enum = MODEL_ENUM.valueOf(processContext.getProperty(MODEL).getValue());
        final DuliModel duliModel;
        if (model_enum.equals(MODEL_ENUM.NONE)) {
            duliModel = DuliModel.valueOf(processContext.getProperty(FIELD).getValue());
        } else {
            duliModel = DuliModel.valueOf(model_enum);
        }

        //get flowfile
        FlowFile flowFile = processSession.get();
        if (flowFile == null) return;
        final List<GenericRecord> outGrCom = new ArrayList<>();
        final List<GenericRecord> outGrSuc = new ArrayList<>();
        //processing
        //read flowfile
        processSession.read(flowFile, in -> {
            final DataFileStream<GenericRecord> reader = new DataFileStream<>(in, new GenericDatumReader<GenericRecord>());
            GenericRecord gr;
            while (reader.hasNext()) {
                gr = reader.next();
//                process(gr, duliModel, initConnection);
            }
        });
        //trans
        FlowFile comFlowFile = processSession.create(flowFile);
        FlowFile sucFlowFile = processSession.create(flowFile);
        processSession.write(comFlowFile, rawOut -> {
            OutputStream bfo = new BufferedOutputStream(rawOut);
            DatumWriter<GenericRecord> writter = new GenericDatumWriter<>();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(bfo, null);
            for (GenericRecord gr : outGrCom) {
                if (gr == null) {
                    logger.error("we got a null record after process");
                    continue;
                }
                writter.write(gr, encoder);
            }
            encoder.flush();
        });
        processSession.write(sucFlowFile, rawOut -> {
            OutputStream bfo = new BufferedOutputStream(rawOut);
            DatumWriter<GenericRecord> writter = new GenericDatumWriter<>();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(bfo, null);
            for (GenericRecord gr : outGrSuc) {
                writter.write(gr, encoder);
            }
            encoder.flush();
        });

        processSession.transfer(comFlowFile, REL_COMPLETE);
        processSession.transfer(sucFlowFile, REL_SUCCESS);
    }
    //一个用以表示预设定的去重模型参数的枚举类
    public enum MODEL_ENUM {
        PERSON,PJ_PERSON,ACHIEVEMENT_DBLP_JNL,ACHIEVEMENT_DBLP_PAPER,ACHIEVEMENT_DBLP_BOOK,ACHIEVEMENT_DBLP_THESIS,ACHIEVEMENT_DBLP_URL
        ,ACHIEVEMENT_DBLP_DATA,ACHIEVEMENT_DBLP_PJ_PERSON,ACHIEVEMENT_ISIS_JNL,ACHIEVEMENT_ISIS_CONF,ACHIEVEMENT_ISIS_BOOK,ACHIEVEMENT_ISIS_PATENT,
        ACHIEVEMENT_ISIS_CONF_REPORT,ACHIEVEMENT_ISIS_STANDARD,ACHIEVEMENT_ISIS_SOFTWARE,ACHIEVEMENT_ISIS_BONUS,ACHIEVEMENT_ISIS_TALENT,ACHIEVEMENT_ISIS_HOST
        ,ACHIEVEMENT_ISIS_TRANS,ACHIEVEMENT_ISIS_OTHER,NONE;
    }

    /**
     * @param genericRecord 需要验证的GR值
     * @param args 需要验证的Main参数
     */
    private static void processMainKey(GenericRecord genericRecord,  String... args) {
        if (isMainKeyDuplicate(genericRecord, args)) {
            updateDB(genericRecord);
        } else {
            processVagueKey(genericRecord, args);
        }
    }

    /**
     * @param genericRecord 需要验证的GR值
     * @param args 需要验证的Vague参数
     */
    private static void processVagueKey(GenericRecord genericRecord,  String... args) {
        if (isVagueKeyDuplicate(genericRecord, args)) {
            updateDB(genericRecord);
        } else {
            insertDB(genericRecord);
        }
    }

    /**
     * @param genericRecord 需要验证的GR
     * @param duliModel 使用的模型
     */
    private void process(GenericRecord genericRecord, DuliModel duliModel, ProcessContext processContext, ProcessSession processSession) {
        Connection connection =  initConnection.apply(processContext, processSession, new FunctionContext(false));
        if (isArgsExist(genericRecord, duliModel.getMainKey().getMain())) {
            processMainKey(genericRecord, duliModel.getMainKey().getMain());
        } else {
            processVagueKey(genericRecord ,duliModel.getVagueKey().getMain());
        }
    }

    /**
     * @param gr 检测的这一列gr
     * @param args 需要验证的main Key参数
     * @return 是否有重复的值
     */
    private static boolean isMainKeyDuplicate(GenericRecord gr, String... args) {
        return true;
    }
    /**
     * @param gr 检测的这一列gr
     * @param args 需要验证的vague Key参数
     * @return 是否有重复的值
     */
    private static boolean isVagueKeyDuplicate(GenericRecord gr, String... args) {
        return true;
    }
    /**
     *
     * @param genericRecord 传入一个待处理的GR值
     * @param args 需要进行检测是否存在的参数
     * @return 这些参数在Avro中是否存在
     */
    private static boolean isArgsExist(GenericRecord genericRecord, String... args) {
        for (String s : args) {
            if (s.contains("&&")) {
                String temp[] = s.split("&&");
                for (String a : temp) {
                    if (genericRecord.get(a) == null || genericRecord.get(a).equals("")) return false;
                }
            } else {
                if (genericRecord.get(s) == null || genericRecord.get(s).equals("")) return false;
            }
        }
        return true;
    }
    public static void updateDB(GenericRecord gr) {}
    public static void insertDB(GenericRecord gr) {}
    //PutSQL中的类，是获取数据库连接的依赖静态类，提供了回滚功能
    private static class FunctionContext extends RollbackOnFailure {
        private boolean obtainKeys = false;
        private boolean fragmentedTransaction = false;
        private boolean originalAutoCommit = false;
        private final long startNanos = System.nanoTime();

        private FunctionContext(boolean rollbackOnFailure) {
            super(rollbackOnFailure, true);
        }

        private boolean isSupportBatching() {
            return !obtainKeys && !fragmentedTransaction;
        }
    }

 }
