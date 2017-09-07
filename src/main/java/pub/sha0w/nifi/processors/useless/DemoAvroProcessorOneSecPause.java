package pub.sha0w.nifi.processors.useless;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class DemoAvroProcessorOneSecPause extends AbstractProcessor {
    private final DataFileWriter<GenericRecord> failureAvroWriter = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>());
    private final DataFileWriter<GenericRecord> successAvroWriter = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>());
    private final Logger logger = LoggerFactory.getLogger(DemoAvroProcessor.class);
    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created FlowFile from HiveQL query result set.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("fail to created FlowFile from HiveQL query result set.")
            .build();

    private final static List<PropertyDescriptor> propertyDescriptors;
    private final static Set<Relationship> relationships;

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);
        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
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
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        //edit flowfile
        FlowFile childFlowFile = session.create(flowFile);
        try {
            Thread.sleep(1000);
            logger.debug("System sleep and pretend to process the flow file");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //submit
        session.transfer(childFlowFile,REL_SUCCESS);
    }

}
