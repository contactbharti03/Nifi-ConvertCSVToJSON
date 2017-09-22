package com.nifi.processors;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.kitesdk.data.DatasetRecordException;
import org.kitesdk.data.spi.filesystem.CSVFileReader;
import org.kitesdk.data.spi.filesystem.CSVProperties;
import org.kitesdk.data.spi.filesystem.CSVUtil;

@Tags({"kite", "csv", "json"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Converts CSV files to JSON")
public class ConvertCsvToJSON extends AbstractProcessor {

	private static final Validator CHAR_VALIDATOR = new Validator() {
		@Override
		public ValidationResult validate(String subject, String input, ValidationContext context) {
			// Allows special, escaped characters as input, which is then
			// unescaped and converted to a single character.
			// Examples for special characters: \t (or \u0009), \f.
			input = unescapeString(input);

			return new ValidationResult.Builder().subject(subject).input(input)
					.explanation("Only non-null single characters are supported")
					.valid(input.length() == 1 && input.charAt(0) != 0 || context.isExpressionLanguagePresent(input))
					.build();
		}
	};

	public static final Pattern AVRO_RECORD_NAME_PATTERN = Pattern.compile("[A-Za-z_]+[A-Za-z0-9_.]*[^.]");

	public static final PropertyDescriptor GET_CSV_HEADER_DEFINITION_FROM_INPUT = new PropertyDescriptor.Builder()
			.name("Get CSV Header Definition From Data")
			.description(
					"This property only applies to CSV content type. If \"true\" the processor will attempt to read the CSV header definition from the"
							+ " first line of the input data.")
			.required(true).allowableValues("true", "false").defaultValue("true")
			.addValidator(StandardValidators.BOOLEAN_VALIDATOR).build();

	public static final PropertyDescriptor CSV_HEADER_DEFINITION = new PropertyDescriptor.Builder()
			.name("CSV Header Definition")
			.description(
					"This property only applies to CSV content type. Comma separated string defining the column names expected in the CSV data."
							+ " EX: \"fname,lname,zip,address\". The elements present in this string should be in the same order"
							+ " as the underlying data. Setting this property will cause the value of" + " \""
							+ GET_CSV_HEADER_DEFINITION_FROM_INPUT.getName()
							+ "\" to be ignored instead using this value.")
			.required(false).expressionLanguageSupported(true).defaultValue(null)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor HEADER_LINE_SKIP_COUNT = new PropertyDescriptor.Builder()
			.name("CSV Header Line Skip Count")
			.description(
					"This property only applies to CSV content type. Specifies the number of lines that should be skipped when reading the CSV data."
							+ " Setting this value to 0 is equivalent to saying \"the entire contents of the file should be read\". If the"
							+ " property \"" + GET_CSV_HEADER_DEFINITION_FROM_INPUT.getName()
							+ "\" is set then the first line of the CSV "
							+ " file will be read in and treated as the CSV header definition. Since this will remove the header line from the data"
							+ " care should be taken to make sure the value of \"CSV header Line Skip Count\" is set to 0 to ensure"
							+ " no data is skipped.")
			.required(true).defaultValue("0").expressionLanguageSupported(true)
			.addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR).build();

	public static final PropertyDescriptor DELIMITER = new PropertyDescriptor.Builder().name("CSV delimiter")
			.description("Delimiter character for CSV records").expressionLanguageSupported(true)
			.addValidator(CHAR_VALIDATOR).defaultValue(",").build();

	public static final PropertyDescriptor ESCAPE = new PropertyDescriptor.Builder().name("CSV Escape String")
			.description("This property only applies to CSV content type. String that represents an escape sequence"
					+ " in the CSV FlowFile content data.")
			.required(true).defaultValue("\\").expressionLanguageSupported(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor QUOTE = new PropertyDescriptor.Builder().name("CSV Quote String")
			.description("This property only applies to CSV content type. String that represents a literal quote"
					+ " character in the CSV FlowFile content data.")
			.required(true).defaultValue("'").expressionLanguageSupported(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder().name("Charset")
			.description("Character encoding of CSV data.").required(true).defaultValue("UTF-8")
			.expressionLanguageSupported(true).addValidator(StandardValidators.CHARACTER_SET_VALIDATOR).build();

	public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
			.description("Successfully created Json File from data.").build();

	public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
			.description("Failed to create Avro schema from data.").build();

	private List<PropertyDescriptor> properties;
	private Set<Relationship> relationships;
	private CSVProperties props;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> properties = new ArrayList<>();
		properties.add(CSV_HEADER_DEFINITION);
		properties.add(GET_CSV_HEADER_DEFINITION_FROM_INPUT);
		properties.add(HEADER_LINE_SKIP_COUNT);
		properties.add(DELIMITER);
		properties.add(ESCAPE);
		properties.add(QUOTE);
		properties.add(CHARSET);
		this.properties = Collections.unmodifiableList(properties);

		final Set<Relationship> relationships = new HashSet<>();
		relationships.add(REL_SUCCESS);
		relationships.add(REL_FAILURE);
		this.relationships = Collections.unmodifiableSet(relationships);
	}

	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return properties;
	}

	@Override
	public Set<Relationship> getRelationships() {
		return relationships;
	}

	@Override
	public void onTrigger(ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile incomingCSV = session.get();
		if (incomingCSV == null) {
			return;
		}

		final Schema schema = inferSchema(context, incomingCSV, session);

		final CSVProperties props = getCSVProperties();
		//final AtomicLong written = new AtomicLong(0L);
		List<String> failures = new ArrayList<String>();
		List<String> goodRecords = new ArrayList<String>();
		
		FlowFile badRecords = session.clone(incomingCSV);
		FlowFile outgoingAvro = session.write(incomingCSV, new StreamCallback() {

			@Override
			public void process(InputStream in, OutputStream out) throws IOException {

				try (CSVFileReader<Record> reader = new CSVFileReader<>(in, props, schema, Record.class)) {
					getLogger().info("schema: "+schema);
					reader.initialize();
					try (final OutputStream output = new BufferedOutputStream(out)) {
						if (reader.hasNext()) {
							output.write('[');
							Record record = reader.next();
							goodRecords.add(record.toString());
							IOUtils.write(record.toString(), output, "UTF-8");
						}
						while (reader.hasNext()) {
							try{
								Record record = reader.next();
								goodRecords.add("a");
								IOUtils.write("," + record.toString(), output, "UTF-8");
							}catch (DatasetRecordException e) {
                                failures.add(e.getMessage());
                            }
							
							
						}
						output.write(']');
					}
				}

			}

		});
		

		session.adjustCounter("Good Records", goodRecords.size(),
                false /* update only if file transfer is successful */);
            session.adjustCounter("Failure", failures.size(),
                false /* update only if file transfer is successful */);
		if(goodRecords.size()>0){
			session.remove(badRecords);
			session.transfer(outgoingAvro, REL_SUCCESS);
		}
		else{
			session.remove(outgoingAvro);
			badRecords = session.putAttribute(
                    badRecords, "errors", failures.get(0));
			session.transfer(badRecords, REL_FAILURE);
		}
			

	}

	private Schema inferSchema(final ProcessContext context, final FlowFile incomingCSV, final ProcessSession session)
			throws ProcessException {
		// Determines the header line either from the property input or the
		// first line of the delimited file.
		final AtomicReference<String> header = new AtomicReference<>();
		final AtomicReference<Boolean> hasHeader = new AtomicReference<>();

		if (context.getProperty(GET_CSV_HEADER_DEFINITION_FROM_INPUT).asBoolean() == Boolean.TRUE) {
			// Read the first line of the file to get the header value.
			session.read(incomingCSV, new InputStreamCallback() {
				@Override
				public void process(InputStream in) throws IOException {
					BufferedReader br = new BufferedReader(new InputStreamReader(in));
					header.set(br.readLine());
					hasHeader.set(Boolean.TRUE);
					br.close();
				}
			});
		} else {
			header.set(context.getProperty(CSV_HEADER_DEFINITION).evaluateAttributeExpressions(incomingCSV).getValue());
			hasHeader.set(Boolean.FALSE);
		}
		

		if (header.get() == null) {
			throw new ProcessException("Header not found");
		}

		final CSVProperties props = new CSVProperties.Builder()
				.charset(context.getProperty(CHARSET).evaluateAttributeExpressions(incomingCSV).getValue())
				.delimiter(context.getProperty(DELIMITER).evaluateAttributeExpressions(incomingCSV).getValue())
				.quote(context.getProperty(QUOTE).evaluateAttributeExpressions(incomingCSV).getValue())
				.escape(context.getProperty(ESCAPE).evaluateAttributeExpressions(incomingCSV).getValue())
				.hasHeader(context.getProperty(GET_CSV_HEADER_DEFINITION_FROM_INPUT)
						.evaluateAttributeExpressions(incomingCSV).asBoolean())
				.header(header.get())
				.linesToSkip(context.getProperty(HEADER_LINE_SKIP_COUNT).evaluateAttributeExpressions(incomingCSV)
						.asInteger())
				.build();

		setCSVProperties(props);

		final AtomicReference<Schema> avroSchema = new AtomicReference<>();

		session.read(incomingCSV, new InputStreamCallback() {
			@Override
			public void process(InputStream in) throws IOException {
				avroSchema.set(CSVUtil.inferSchema("recordName", in,
						props));
			}
		});

		return avroSchema.get();

	}

	public void setCSVProperties(CSVProperties props) {
		this.props = props;
	}

	private CSVProperties getCSVProperties() {
		return this.props;
	}

	private static String unescapeString(String input) {
		if (input.length() > 1) {
			input = StringEscapeUtils.unescapeJava(input);
		}
		return input;
	}

}
