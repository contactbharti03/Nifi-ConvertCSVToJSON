import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import com.nifi.processors.ConvertCsvToJSON;

public class TestConvertCsvToJSON {
	
	@Test
    public void testCommaSeparatedConversion() throws IOException {
		TestRunner runner = TestRunners.newTestRunner(ConvertCsvToJSON.class);
        runner.assertNotValid();
        runner.setProperty(ConvertCsvToJSON.HEADER_LINE_SKIP_COUNT, "0");
        runner.setProperty(ConvertCsvToJSON.RECORD_NAME, "elections");
        runner.assertValid();
        runner.setValidateExpressionUsage(false);
        runner.enqueue(new FileInputStream(new File("src/test/resources/Test.csv")));
        runner.run();
        
        runner.assertAllFlowFilesTransferred(ConvertCsvToJSON.REL_SUCCESS, 1);
    	List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(ConvertCsvToJSON.REL_SUCCESS);
    
    	for(MockFlowFile mockFile : successFiles){
    		System.out.println(new String(mockFile.toByteArray(), "UTF-8"));
    	}
	}
}
