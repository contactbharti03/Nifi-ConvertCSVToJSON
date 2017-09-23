package com.nifi.processors;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;

public class TestConvertCsvToJSON {
	
	@Test
    public void testCommaSeparatedConversionWithHeader() throws IOException {
		TestRunner runner = TestRunners.newTestRunner(ConvertCsvToJSON.class);
		runner.setProperty(ConvertCsvToJSON.GET_CSV_HEADER_DEFINITION_FROM_INPUT, "true");
        runner.setValidateExpressionUsage(false);
        runner.enqueue(new FileInputStream(new File("src/test/resources/Test.csv")));
        runner.run();
        
        runner.assertAllFlowFilesTransferred(ConvertCsvToJSON.SUCCESS, 1);
        long goodRecords = runner.getCounterValue("Good Records");
        long badRecords = runner.getCounterValue("Failure");
        Assert.assertEquals("Converts 2 rows", 2, goodRecords);
        Assert.assertEquals("Zero Failures", 0, badRecords);
        
	}
	
	@Test
    public void testCommaSeparatedConversionWithoutHeader() throws IOException {
		TestRunner runner = TestRunners.newTestRunner(ConvertCsvToJSON.class);
        runner.setProperty(ConvertCsvToJSON.GET_CSV_HEADER_DEFINITION_FROM_INPUT, "false");
        runner.setProperty(ConvertCsvToJSON.CSV_HEADER_DEFINITION, "state,state_abbreviation,county,fips,party,candidate,votes,fraction_votes");
        runner.setValidateExpressionUsage(false);
        runner.enqueue(new FileInputStream(new File("src/test/resources/NoHeader.csv")));
        runner.run();
        
        runner.assertAllFlowFilesTransferred(ConvertCsvToJSON.SUCCESS, 1);
        long goodRecords = runner.getCounterValue("Good Records");
        long badRecords = runner.getCounterValue("Failure");
        Assert.assertEquals("Converts 2 rows", 2, goodRecords);
        Assert.assertEquals("Zero Failures", 0, badRecords);
	}
}
