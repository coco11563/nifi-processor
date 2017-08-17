package pub.sha0w.nifi.processors;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pub.sha0w.nifi.processors.model.DuliModel;


import java.util.*;

public class DemoDuplicateRemoveProcessor extends AbstractProcessor {
    /**
     * {@link Validator} that ensures that value contain ";"
     */
    private static final Validator RULE_VALID_VALIDATOR = (subject, value, context) -> new ValidationResult.Builder().subject(subject).input(value).valid(value.contains(";")).explanation(subject + " must contain a \";\"").build();

    private final Logger logger = LoggerFactory.getLogger(DemoDuplicateRemoveProcessor.class);
    // Relationships
    private static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully trans FlowFile")
            .build();
    private static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("fail to trans FlowFile")
            .build();

    //PropertiesDescriptor
    public static final PropertyDescriptor MODEL = new PropertyDescriptor.Builder()
            .name("model")
            .description("由一系列预设置的去重字段组成，如果为“NONE”时则调用“FIELD”中输入的由逗号分隔的字符串作为去重关键字")
            .required(true)
            .defaultValue("NONE")
            .allowableValues(MODEL_ENUM.values())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();  //PropertiesDescriptor

    public static final PropertyDescriptor FIELD = new PropertyDescriptor.Builder()
            .name("field")
            .description("若MODEL中的数据为“NONE”则会从这个输入的字符串中获取所需要的参数。输入的参数" +
                    "的详细格式规则：条件内容用“，”分隔，详细条件与模糊条件按照“；”来进行分隔，与和或以“&”和“|”进行标识")
            .addValidator(RULE_VALID_VALIDATOR)
            .required(false)
            .build();
    private final static List<PropertyDescriptor> propertyDescriptors;
    private final static Set<Relationship> relationships;

    static {
        List<org.apache.nifi.components.PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(MODEL);
        _propertyDescriptors.add(FIELD);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);
        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }


    @Override
    protected List<org.apache.nifi.components.PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {
        //init process way
        DuliModel model_enum = DuliModel.valueOf(MODEL_ENUM.valueOf(processContext.getProperty(MODEL).getValue()));
        logger.debug(Arrays.toString(model_enum.getMainKey().getMain()));
        logger.debug(Arrays.toString(model_enum.getVagueKey().getMain()));
        //get flowfile
        //processing
        //trans
    }

    public enum MODEL_ENUM {
        PERSON,PJ_PERSON,ACHIEVEMENT_DBLP_JNL,ACHIEVEMENT_DBLP_PAPER,ACHIEVEMENT_DBLP_BOOK,ACHIEVEMENT_DBLP_THESIS,ACHIEVEMENT_DBLP_URL
        ,ACHIEVEMENT_DBLP_DATA,ACHIEVEMENT_DBLP_PJ_PERSON,ACHIEVEMENT_ISIS_JNL,ACHIEVEMENT_ISIS_CONF,ACHIEVEMENT_ISIS_BOOK,ACHIEVEMENT_ISIS_PATENT,
        ACHIEVEMENT_ISIS_CONF_REPORT,ACHIEVEMENT_ISIS_STANDARD,ACHIEVEMENT_ISIS_SOFTWARE,ACHIEVEMENT_ISIS_BONUS,ACHIEVEMENT_ISIS_TALENT,ACHIEVEMENT_ISIS_HOST
        ,ACHIEVEMENT_ISIS_TRANS,ACHIEVEMENT_ISIS_OTHER,NONE;
    }

 }
