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

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters.FailsafeJsonToTableRow;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.MoreObjects;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;


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
    public interface BigQuerySchemaFromJsonOptions extends PipelineOptions {


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
//        coderRegistry.registerCoderForType(TableSchemaCoder.of().getEncodedTypeDescriptor(), TableSchemaCoder.of());

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
        jsonToTableRowOutput.get(TRANSFORM_OUT).apply(Combine.globally(new SchemaFnCombiner()));


        return pipeline.run();
    }

    /**
     * The {@link ProcessSchema} class process the schema.
     */
    static class ProcessSchema extends DoFn<KV<Integer, TableRow>, TableRow> {

        @StateId("count")
        private final StateSpec<ValueState<Long>> countState = StateSpecs.value();

        @StateId("tableSchema")
        private final StateSpec<ValueState<TableSchema>> tableSchema = StateSpecs.value(TableSchemaCoder.of());


        private Long maxLinesToProcess;


        ProcessSchema(Long maxLinesToProcess) {
            this.maxLinesToProcess = maxLinesToProcess;
        }

        public TableFieldSchema getSchemaField(String path, String key, Object value) {
            TableFieldSchema schemaField = new TableFieldSchema();
            String currentPath = MoreObjects.firstNonNull(path, "");
            if (currentPath != "") {
                currentPath += "." + key;
            } else {
                currentPath = key;
            }

            if (value instanceof String) {
                System.out.println("Path: " + currentPath + " Value: STRING");
                schemaField.setType("STRING");
            } else if (value instanceof Integer) {
                System.out.println("Path: " + currentPath + " Value: INTEGER");
                schemaField.setType("INTEGER");
            } else if (value instanceof Double) {
                System.out.println("Path: " + currentPath + "Value: DOUBLE");
                schemaField.setMode("DOUBLE");
            } else if (value instanceof LinkedHashMap) {
                System.out.println("Path: " + currentPath + " Value: STRUCT (RECORD)");
                schemaField.setType("RECORD");
                LinkedHashMap<String, Object> linkedHashMap = (LinkedHashMap<String, Object>) value;
                for (String linkedKey : ((LinkedHashMap<String, Object>) value).keySet()) {
                    getSchemaField(currentPath, linkedKey, linkedHashMap.get(linkedKey));
                }
            } else if (value instanceof ArrayList) {

                schemaField.setMode("REPEATED");
                List<Object> arrayList = (ArrayList<Object>) value;
                if (arrayList.size() > 0) {
                    Object o = arrayList.get(0);
                    System.out.println("Array Type Class:" + o.getClass());
                    if (o instanceof LinkedHashMap) {
                        System.out.println("Path: " + currentPath + " Value: REPEATED (ARRAY)-STRUCT");
                        getSchemaField(path, key, o);
                    } else if (o instanceof String) {
                        System.out.println("Path: " + currentPath + " Value: REPEATED (ARRAY)-STRING");
                    } else if (o instanceof Integer) {
                        System.out.println("Path: " + currentPath + " Value: REPEATED (ARRAY)-INTEGER");
                    } else if (o instanceof Double) {
                        System.out.println("Path: " + currentPath + " Value: REPEATED (ARRAY)-DOUBLE");
                    }
                }
            }

            return schemaField;
        }

        @ProcessElement
        public void processElement(ProcessContext c,
                                   @StateId("count") ValueState<Long> countState,
                                   @StateId("tableSchema") ValueState<TableSchema> tableSchemaState) {

            TableSchema tableSchema = MoreObjects.firstNonNull(tableSchemaState.read(), new TableSchema());
            Long count = MoreObjects.firstNonNull(countState.read(), new Long(0));
            TableRow tableRow = c.element().getValue();
            for (String key : tableRow.keySet()) {
//                System.out.println("Key: " + key + " Value: " + tableRow.get(key)  + " Class: "+ tableRow.get(key).getClass());
                getSchemaField(null, key, tableRow.get(key));
            }
            count++;
//            System.out.printf("New Count: " + count + "\n");
            System.out.printf("--------------------------------------------------\n");
            countState.write(count);


        }

    }


}
