package com.google.cloud.teleport.v2.templates;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.TypeDescriptor;

public class TableSchemaCoder extends AtomicCoder<TableSchema> {

    public static TableSchemaCoder of() {
        return INSTANCE;
    }

    private final StringUtf8Coder stringCoder = StringUtf8Coder.of();

    @Override
    public void encode(TableSchema value, OutputStream outStream) throws IOException {
        encode(value, outStream, Context.NESTED);
    }

    @Override
    public void encode(TableSchema value, OutputStream outStream, Context context) throws IOException {
        String strValue = MAPPER.writeValueAsString(value);
        StringUtf8Coder.of().encode(strValue, outStream, context);
    }

    @Override
    public TableSchema decode(InputStream inStream) throws IOException {
        return decode(inStream, Context.NESTED);
    }

    @Override
    public TableSchema decode(InputStream inStream, Context context) throws IOException {
        String strValue = StringUtf8Coder.of().decode(inStream, context);
        return MAPPER.readValue(strValue, TableSchema.class);
    }

    @Override
    public long getEncodedElementByteSize(TableSchema value) throws Exception {
        String strValue = MAPPER.writeValueAsString(value);
        return StringUtf8Coder.of().getEncodedElementByteSize(strValue);
    }
    /////////////////////////////////////////////////////////////////////////////

    // FAIL_ON_EMPTY_BEANS is disabled in order to handle null values in
    // TableRow.
    private static final ObjectMapper MAPPER =
            new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

    private static final TableSchemaCoder INSTANCE = new TableSchemaCoder();
    private static final TypeDescriptor<TableSchema> TYPE_DESCRIPTOR = new TypeDescriptor<TableSchema>() {};

    private TableSchemaCoder() {}

    /**
     * {@inheritDoc}
     *
     * @throws NonDeterministicException always. A {@link TableRow} can hold arbitrary {@link Object}
     *     instances, which makes the encoding non-deterministic.
     */
    @Override
    public void verifyDeterministic() throws NonDeterministicException {
        throw new NonDeterministicException(
                this, "TableCell can hold arbitrary instances, which may be non-deterministic.");
    }

    @Override
    public TypeDescriptor<TableSchema> getEncodedTypeDescriptor() {
        return TYPE_DESCRIPTOR;
    }
}
