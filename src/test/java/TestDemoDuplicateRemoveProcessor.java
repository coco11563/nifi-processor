
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
import pub.sha0w.nifi.processors.useless.DemoDuplicateRemoveProcessor;

public class TestDemoDuplicateRemoveProcessor {
    /**
     * test passed
     */
    @Test
    public void initTest() {
        TestRunner testRunner = TestRunners.newTestRunner(new DemoDuplicateRemoveProcessor());
        testRunner.setProperty(DemoDuplicateRemoveProcessor.FIELD, "url;TITLE,AUTHOR,YEAR");
        for (AllowableValue allowableValue : DemoDuplicateRemoveProcessor.MODEL.getAllowableValues()) {
            testRunner.setProperty(DemoDuplicateRemoveProcessor.MODEL, allowableValue);
            testRunner.run();
        }
    }
}
