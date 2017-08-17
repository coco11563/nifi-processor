
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import pub.sha0w.nifi.processors.DemoDuplicateRemoveProcessor;

public class TestDemoDuplicateRemoveProcessor {
    static TestRunner testRunner = TestRunners.newTestRunner(new DemoDuplicateRemoveProcessor());
    @Before
    public void init(){
        testRunner.setProperty(DemoDuplicateRemoveProcessor.MODEL, DemoDuplicateRemoveProcessor.MODEL.getAllowableValues().get(0));
    }
    @Test
    public void initTest() {
        testRunner.run();
    }
}
