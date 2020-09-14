/*
 * Copyright (C) 2020 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.teleport.v2.templates;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableList;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters.FailsafeJsonToTableRow;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.auth.NullCredentialInitializer;
import org.apache.beam.sdk.extensions.gcp.util.RetryHttpRequestInitializer;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;


/**
 * The {@link BigQuerySchemaFromJson} is a streaming pipeline which generates messages at a specified
 * rate to a Pub/Sub topic. The messages are generated according to a schema template which
 * instructs the pipeline how to populate the messages with fake data compliant to constraints.
 *
 * <p>The number of workers executing the pipeline must be large enough to support the supplied QPS.
 * Use a general rule of 2,500 QPS per core in the worker pool.
 *
 * <p>See <a href="https://github.com/vincentrussell/json-data-generator">json-data-generator</a>
 * for instructions on how to construct the schema file.
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>The schema file exists.
 *   <li>The Pub/Sub topic exists.
 * </ul>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 * # Set the pipeline vars
 * PROJECT=my-project
 * BUCKET_NAME=my-bucket
 * SCHEMA_LOCATION=gs://bucket/path/to/game-event-schema.json
 * PUBSUB_TOPIC=projects/project-id/topics/topic-id
 * QPS=2500
 *
 * # Set containerization vars
 * IMAGE_NAME=my-image-name
 * TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
 * BASE_CONTAINER_IMAGE=my-base-container-image
 * BASE_CONTAINER_IMAGE_VERSION=my-base-container-image-version
 * APP_ROOT=/path/to/app-root
 * COMMAND_SPEC=/path/to/command-spec
 *
 * # Build and upload image
 * mvn clean package \
 * -Dimage=${TARGET_GCR_IMAGE} \
 * -Dbase-container-image=${BASE_CONTAINER_IMAGE} \
 * -Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
 * -Dapp-root=${APP_ROOT} \
 * -Dcommand-spec=${COMMAND_SPEC}
 *
 * # Create a template spec containing the details of image location and metadata in GCS
 *   as specified in README.md file
 *
 * # Execute template:
 * JOB_NAME=job-name
 * PROJECT=project-id
 * TEMPLATE_SPEC_GCSPATH=gs://path/to/template-spec
 * SCHEMA_LOCATION=gs://path/to/schema.json
 * QPS=1
 *
 * gcloud beta dataflow jobs run $JOB_NAME \
 *         --project=$PROJECT --region=us-central1 --flex-template  \
 *         --gcs-location=$TEMPLATE_SPEC_GCSPATH \
 *         --parameters autoscalingAlgorithm="THROUGHPUT_BASED",schemaLocation=$SCHEMA_LOCATION,topic=$PUBSUB_TOPIC,qps=$QPS,maxNumWorkers=3
 * </pre>
 */
public class BigQuerySchemaFromJson {


    /**
     * String/String Coder for FailsafeElement.
     */
    public static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
            FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());


    /**
     * The tag for the main output of the json transformation.
     */
    static final TupleTag<TableRow> TRANSFORM_OUT = new TupleTag<TableRow>() {
    };

    /**
     * The tag for the dead-letter output of the udf.
     */
    static final TupleTag<FailsafeElement<String, String>> DEADLETTER_OUT = new TupleTag<FailsafeElement<String, String>>() {
    };

    /**
     * The {@link BigQuerySchemaFromJsonOptions} class provides the custom execution options passed by the executor at the
     * command-line.
     */
    public interface BigQuerySchemaFromJsonOptions extends PipelineOptions, BigQueryOptions {


        @Description("Maximum number of lines to Test, defaults to unlimited (0).")
        @Default.Long(0)
        @Required
        Long getMaxLinesToTest();

        void setMaxLinesToTest(Long maxLinesToTest);

        @Description("Scan a limited number of lines to probe the schema, default is all.")
        String getInputFilePattern();

        void setInputFilePattern(String inputFilePattern);


        @Description("BigQuery Table to Mutate with Updated Schema.")
        String getBigQueryTableToMutate();

        void setBigQueryTableToMutate(String bigQueryTableToMutate);


        @Description("BigQuery Dataset")
        String getBigQueryDataset();

        void setBigQueryDataset(String bigQueryDataset);


    }

    /**
     * The main entry-point for pipeline execution. This method will start the pipeline but will not
     * wait for it's execution to finish. If blocking execution is required, use the {@link
     * BigQuerySchemaFromJson#run(BigQuerySchemaFromJsonOptions)} method to start the pipeline and invoke {@code
     * result.waitUntilFinish()} on the {@link PipelineResult}.
     *
     * @param args The command-line args passed by the executor.
     */
    public static void main(String[] args) {
        BigQuerySchemaFromJsonOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(BigQuerySchemaFromJsonOptions.class);
        run(options);
    }

    /**
     * Get Raw Type based on an hierarchy.
     *
     * @param arrayTypes
     * @return
     */
    public static String getRawTypeHierarchy(List<String> arrayTypes) {
        //Default Type to Return by priority
        String returnType = SchemaFnCombiner.FIELD_STRING;
        if (arrayTypes.contains(SchemaFnCombiner.FIELD_RECORD)) {
            returnType = SchemaFnCombiner.FIELD_RECORD;
        } else if (arrayTypes.contains(SchemaFnCombiner.FIELD_STRING)) {
            returnType = SchemaFnCombiner.FIELD_STRING;
        } else if (arrayTypes.contains(SchemaFnCombiner.FIELD_DOUBLE)) {
            returnType = SchemaFnCombiner.FIELD_DOUBLE;
        } else if (arrayTypes.contains(SchemaFnCombiner.FIELD_INTEGER)) {
            returnType = SchemaFnCombiner.FIELD_INTEGER;
        }
        return returnType;
    }


    /**
     * Generate BigQuery Field Schema.
     *
     * @param name
     * @param potentialTypes
     * @return
     */
    public static TableFieldSchema generateFieldSchema(String name, HashMap<String, Long> potentialTypes) {
        TableFieldSchema field = new TableFieldSchema();
        field.setName(name);

        List<String> allTypes = new ArrayList<>();
        List<String> arrayTypes = new ArrayList<>();

        boolean hasArray = false;
        for (String types : potentialTypes.keySet()) {
            if (types.contains(SchemaFnCombiner.FIELD_ARRAY)) {
                String arrayType = types.substring(types.indexOf(":") + 1);
                System.out.println("Repeated Type: " + arrayType);
                arrayTypes.add(arrayType);
                hasArray = true;
            } else {
                allTypes.add(types);
            }
        }
        if (hasArray) {
            field.setMode("REPEATED");
            field.setType(getRawTypeHierarchy(arrayTypes));
        } else {
            field.setType(getRawTypeHierarchy(allTypes));
        }

        return field;
    }

    /**
     * This method generates BigQuery schema.
     *
     * @param tableSchema
     * @param parentFieldSchema
     * @param map
     */
    public static void generateSchema(TableSchema tableSchema, TableFieldSchema parentFieldSchema, TreeMap<String, HashMap<String, Long>> map) {

        TreeMap<String, TableFieldSchema> generatedMap = new TreeMap<>();
        List<String> processedFields = new ArrayList<>();

        for (String key : map.keySet()) {
            //Easy basic field
            if (!key.contains(".")) {
                TableFieldSchema genField = generateFieldSchema(key, map.get(key));
                generatedMap.put(key, genField);
                if (parentFieldSchema == null) {
                    //Initialize Fields if null
                    if (tableSchema.getFields() == null) {
                        tableSchema.setFields(new ArrayList<>());
                    }
                    tableSchema.getFields().add(genField);
                } else {
                    //Initialize Fields if null
                    if (parentFieldSchema.getFields() == null) {
                        parentFieldSchema.setFields(new ArrayList<>());
                    }
                    parentFieldSchema.getFields().add(genField);
                }
                processedFields.add(key);
            }
            //this can be array or struct
            else {
                //if field has not been processed
                if (!processedFields.contains(key)) {
                    //root
                    processedFields.add(key);
                    String prefix = key.substring(0, key.indexOf("."));
                    System.out.println("Prefix: " + prefix);
                    TreeMap<String, HashMap<String, Long>> internalMap = new TreeMap<>();
                    for (String keyInternal : map.keySet()) {
                        //get all prefix
                        if (!keyInternal.equals(prefix) && keyInternal.contains(prefix)
                                && keyInternal.contains(".") && keyInternal.substring(0, keyInternal.indexOf(".")).equals(prefix)) {

                            String nextToken = keyInternal.substring(keyInternal.indexOf(".") + 1);
                            internalMap.put(nextToken, map.get(keyInternal));
                            processedFields.add(keyInternal);
                        }
                    }
                    generateSchema(tableSchema, generatedMap.get(prefix), internalMap);
                }
            }
        }
    }

    /**
     * Create chain Request initializer.
     *
     * @param credential
     * @param httpRequestInitializer
     * @return
     */
    private static HttpRequestInitializer chainHttpRequestInitializer(
            Credentials credential, HttpRequestInitializer httpRequestInitializer) {
        if (credential == null) {
            return new ChainingHttpRequestInitializer(
                    new NullCredentialInitializer(), httpRequestInitializer);
        } else {
            return new ChainingHttpRequestInitializer(
                    new HttpCredentialsAdapter(credential), httpRequestInitializer);
        }
    }

    /**
     * Runs the pipeline to completion with the specified options. This method does not wait until the
     * pipeline is finished before returning. Invoke {@code result.waitUntilFinish()} on the result
     * object to block until the pipeline is finished running if blocking programmatic execution is
     * required.
     *
     * @param options The execution options.
     * @return The pipeline result.
     */
    public static PipelineResult run(BigQuerySchemaFromJsonOptions options) {

        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);
        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderForType(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);
        /*
         * Steps:
         *  1) Read File to Load to Generate Schema To Update
         *  2) Try to update or create the schema based on the table name supplied
         *  3) If not possible, throw errors
         */
        PCollectionTuple jsonToTableRowOutput = pipeline
                .apply(
                        "Read JSON Delimitted Files", TextIO.read().from(options.getInputFilePattern()))
                .apply("StringToFailsafeJson", ParDo.of(new DoFn<String, FailsafeElement<String, String>>() {

                    @ProcessElement
                    public void processElement(ProcessContext context) {
                        String element = context.element();
                        context.output(FailsafeElement.of(element, element));
                    }
                }))
                .apply("JsonToTableRow",
                        FailsafeJsonToTableRow.<String>newBuilder()
                                .setSuccessTag(TRANSFORM_OUT)
                                .setFailureTag(DEADLETTER_OUT)
                                .build());

        jsonToTableRowOutput.get(TRANSFORM_OUT).setCoder(TableRowJsonCoder.of());
        jsonToTableRowOutput.get(DEADLETTER_OUT).setCoder(FAILSAFE_ELEMENT_CODER);
        jsonToTableRowOutput.get(TRANSFORM_OUT).apply(Combine.globally(new SchemaFnCombiner()))
                .apply("GenerateSchemaApplyTable", ParDo.of(new DoFn<TreeMap<String, HashMap<String, Long>>, Void>() {


                    @ProcessElement
                    public void processElement(ProcessContext context) {
                        TableSchema tableSchema = new TableSchema();
                        BigQuerySchemaFromJsonOptions options = context.getPipelineOptions().as(BigQuerySchemaFromJsonOptions.class);
                        TreeMap<String, HashMap<String, Long>> map = context.element();
                        generateSchema(tableSchema, null, map);
                        System.out.printf("TableSchema" + tableSchema);
                        RetryHttpRequestInitializer httpRequestInitializer =
                                new RetryHttpRequestInitializer(ImmutableList.of(404));

                        Bigquery bigquery = new Bigquery.Builder(Transport.getTransport(), Transport.getJsonFactory(), chainHttpRequestInitializer(
                                options.getGcpCredential(),
                                // Do not log 404. It clutters the output and is possibly even required by the
                                // caller.
                                httpRequestInitializer))
                                .setApplicationName(options.getAppName())
                                .setGoogleClientRequestInitializer(options.getGoogleApiTrace()).build();


                        try {
                            TableReference tableRef = new TableReference();
                            tableRef.setTableId(options.getBigQueryTableToMutate());
                            tableRef.setDatasetId(options.getBigQueryDataset());
                            tableRef.setProjectId(options.getProject());
                            Table table = new Table().setTableReference(tableRef).setSchema(tableSchema).setDescription("Dynamic Json Table");

                            boolean tableExists = false;
                            //Try to list the tables
                            TableList listedTables = bigquery.tables().list(options.getProject(), options.getBigQueryDataset()).execute();
                            for(TableList.Tables listTable: listedTables.getTables())
                            {
                                if(listTable.getTableReference().getTableId().equals(options.getBigQueryTableToMutate()))
                                {
                                    tableExists = true;
                                    break;
                                }
                            }
                            //Try to update the schema
                            if(tableExists)
                            {
                                bigquery.tables().update(options.getProject(), options.getBigQueryDataset(), options.getBigQueryTableToMutate(), table);
                                System.out.println("\nTable updated successfully");
                            }
                            else {
                                bigquery.tables().insert(options.getProject(), options.getBigQueryDataset(), table).execute();
                                System.out.println("\nTable created successfully");
                            }

                        } catch (IOException e) {
                            System.err.println("\nFail to create or update table");
                            e.printStackTrace();

                        }


                    }

                }));


        return pipeline.run();
    }


}
