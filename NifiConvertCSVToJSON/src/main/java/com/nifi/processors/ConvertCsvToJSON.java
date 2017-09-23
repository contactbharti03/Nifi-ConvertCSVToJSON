package com.nifi.processors;

import static org.apache.nifi.processor.util.StandardValidators.createLongValidator;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.kitesdk.data.DatasetRecordException;
import org.kitesdk.data.spi.filesystem.CSVFileReader;
import org.kitesdk.data.spi.filesystem.CSVProperties;
import org.kitesdk.data.spi.filesystem.CSVUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

@Tags({"kite", "csv", "json"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Converts CSV files to JSON")
public class ConvertCsvToJSON extends AbstractProcessor  {
	
	private static final CSVProperties DEFAULTS = new CSVProperties.Builder().build();

    private static final Validator CHAR_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            // Allows special, escaped characters as input, which is then unescaped and converted to a single character.
            // Examples for special characters: \t (or \u0009), \f.
            input = unescapeString(input);

            return new ValidationResult.Builder()
                .subject(subject)
                .input(input)
                .explanation("Only non-null single characters are supported")
                .valid((input.length() == 1 && input.charAt(0) != 0) || context.isExpressionLanguagePresent(input))
                .build();
        }
    };

    static final Relationship SUCCESS = new Relationship.Builder()
        .name("success")
        .description("Avro content that was converted successfully from CSV")
        .build();

    static final Relationship FAILURE = new Relationship.Builder()
        .name("failure")
        .description("CSV content that could not be processed")
        .build();

    @VisibleForTesting
    static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
        .name("CSV charset")
        .description("Character set for CSV files")
        .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
        .expressionLanguageSupported(true)
        .defaultValue(DEFAULTS.charset)
        .build();

    @VisibleForTesting
    static final PropertyDescriptor DELIMITER = new PropertyDescriptor.Builder()
        .name("CSV delimiter")
        .description("Delimiter character for CSV records")
        .addValidator(CHAR_VALIDATOR)
        .expressionLanguageSupported(true)
        .defaultValue(DEFAULTS.delimiter)
        .build();

    @VisibleForTesting
    static final PropertyDescriptor QUOTE = new PropertyDescriptor.Builder()
        .name("CSV quote character")
        .description("Quote character for CSV values")
        .addValidator(CHAR_VALIDATOR)
        .expressionLanguageSupported(true)
        .defaultValue(DEFAULTS.quote)
        .build();

    @VisibleForTesting
    static final PropertyDescriptor ESCAPE = new PropertyDescriptor.Builder()
        .name("CSV escape character")
        .description("Escape character for CSV values")
        .addValidator(CHAR_VALIDATOR)
        .expressionLanguageSupported(true)
        .defaultValue(DEFAULTS.escape)
        .build();

    @VisibleForTesting
    static final PropertyDescriptor GET_CSV_HEADER_DEFINITION_FROM_INPUT = new PropertyDescriptor.Builder()
        .name("Use CSV header line")
        .description("Whether to use the first line as a header")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .expressionLanguageSupported(true)
        .defaultValue(String.valueOf(DEFAULTS.useHeader))
        .build();

    @VisibleForTesting
    static final PropertyDescriptor LINES_TO_SKIP = new PropertyDescriptor.Builder()
        .name("Lines to skip")
        .description("Number of lines to skip before reading header or data")
        .addValidator(createLongValidator(0L, Integer.MAX_VALUE, true))
        .expressionLanguageSupported(true)
        .defaultValue(String.valueOf(DEFAULTS.linesToSkip))
        .build();
    
    @VisibleForTesting
    static final PropertyDescriptor CSV_HEADER_DEFINITION = new PropertyDescriptor.Builder()
			.name("CSV Header Definition")
			.description(
					"This property only applies to CSV content type. Comma separated string defining the column names expected in the CSV data."
							+ " EX: \"fname,lname,zip,address\". The elements present in this string should be in the same order"
							+ " as the underlying data. Setting this property will cause the value of" + " \""
							+ GET_CSV_HEADER_DEFINITION_FROM_INPUT.getName()
							+ "\" to be ignored instead using this value.")
			.required(false).expressionLanguageSupported(true).defaultValue(null)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    private static final List<PropertyDescriptor> PROPERTIES = ImmutableList.<PropertyDescriptor> builder()
        .add(CHARSET)
        .add(DELIMITER)
        .add(QUOTE)
        .add(ESCAPE)
        .add(GET_CSV_HEADER_DEFINITION_FROM_INPUT)
        .add(LINES_TO_SKIP)
        .add(CSV_HEADER_DEFINITION)
        .build();

    private static final Set<Relationship> RELATIONSHIPS = ImmutableSet.<Relationship> builder()
        .add(SUCCESS)
        .add(FAILURE)
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

	private CSVProperties props;


	@Override
	public void onTrigger(ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile incomingCSV = session.get();
		if (incomingCSV == null) {
			return;
		}
		
		final Schema schema = inferSchema(context, incomingCSV, session);

		final CSVProperties props = getCSVProperties();
		
		List<String> failures = new ArrayList<String>();
		final AtomicLong goodRecords = new AtomicLong(0L);
		
		FlowFile badRecords = session.clone(incomingCSV);
		FlowFile outgoingAvro = session.write(incomingCSV, new StreamCallback() {

			@Override
			public void process(InputStream in, OutputStream out) throws IOException {

				try (CSVFileReader<Record> reader = new CSVFileReader<Record>(in, props, schema, Record.class)) {
					getLogger().info("schema: "+schema);
					reader.initialize();
					try (final OutputStream output = new BufferedOutputStream(out)) {
						if (reader.hasNext()) {
							output.write('[');
							Record record = reader.next();
							goodRecords.incrementAndGet();
							IOUtils.write(record.toString(), output, DEFAULTS.charset);
						}
						while (reader.hasNext()) {
							try{
								Record record = reader.next();
								goodRecords.incrementAndGet();
								IOUtils.write("," + record.toString(), output, DEFAULTS.charset);
							}catch (DatasetRecordException e) {
                                failures.add(e.getMessage());
                            }
							
							
						}
						output.write(']');
					}
				}

			}

		});
		
		session.adjustCounter("Good Records", goodRecords.get(),
                false /* update only if file transfer is successful */);
            session.adjustCounter("Failure", failures.size(),
                false /* update only if file transfer is successful */);
		if(goodRecords.get()>0L){
			session.remove(badRecords);
			session.transfer(outgoingAvro, SUCCESS);
		}
		else{
			session.remove(outgoingAvro);
			badRecords = session.putAttribute(
                    badRecords, "errors", failures.get(0));
			session.transfer(badRecords, FAILURE);
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
				.linesToSkip(context.getProperty(LINES_TO_SKIP).evaluateAttributeExpressions(incomingCSV)
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
