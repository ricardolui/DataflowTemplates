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

import com.github.vincentrussell.json.datagenerator.JsonDataGenerator;
import com.github.vincentrussell.json.datagenerator.JsonDataGeneratorException;
import com.github.vincentrussell.json.datagenerator.impl.JsonDataGeneratorImpl;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.vendor.guava.v20_0.com.google.common.io.ByteStreams;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@link StreamingDataGenerator} is a streaming pipeline which generates messages at a specified
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
 * ATTRIBUTE_SCHEMA_LOCATION=gs://bucket/path/to/attribute-game-event-schema.json
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
 * ATTRIBUTE_SCHEMA_LOCATION=gs://path/to/attribute-schema.json
 * PUBSUB_TOPIC=projects/$PROJECT/topics/topic-name
 * QPS=1
 *
 * gcloud beta dataflow jobs run $JOB_NAME \
 *         --project=$PROJECT --region=us-central1 --flex-template  \
 *         --gcs-location=$TEMPLATE_SPEC_GCSPATH \
 *         --parameters autoscalingAlgorithm="THROUGHPUT_BASED",schemaLocation=$SCHEMA_LOCATION,attributeSchemaLocation=$ATTRIBUTE_SCHEMA_LOCATION,topic=$PUBSUB_TOPIC,qps=$QPS,maxNumWorkers=3
 * </pre>
 */
public class StreamingDataGenerator {


    /**
     * The {@link StreamingDataGeneratorOptions} class provides the custom execution options passed by the executor at the
     * command-line.
     */
    public interface StreamingDataGeneratorOptions extends PipelineOptions {
        @Description("Indicates rate of messages per second to be published to Pub/Sub.")
        @Required
        Long getQps();

        void setQps(Long value);

        @Description("The path to the schema to generate.")
        @Required
        String getSchemaLocation();

        void setSchemaLocation(String value);


        @Description("The path to the attribute schema to generate.")
        String getAttributeSchemaLocation();

        void setAttributeSchemaLocation(String value);


        @Description("The Pub/Sub topic to write to.")
        @Required
        String getTopic();

        void setTopic(String value);
    }

    /**
     * The main entry-point for pipeline execution. This method will start the pipeline but will not
     * wait for it's execution to finish. If blocking execution is required, use the {@link
     * StreamingDataGenerator#run(StreamingDataGeneratorOptions)} method to start the pipeline and invoke {@code
     * result.waitUntilFinish()} on the {@link PipelineResult}.
     *
     * @param args The command-line args passed by the executor.
     */
    public static void main(String[] args) {

        StreamingDataGeneratorOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(StreamingDataGeneratorOptions.class);

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
    public static PipelineResult run(StreamingDataGeneratorOptions options) {

        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);

        /*
         * Steps:
         *  1) Trigger at the supplied QPS
         *  2) Generate messages containing fake data
         *  3) Write messages to Pub/Sub
         */
        pipeline
                .apply(
                        "Trigger",
                        GenerateSequence.from(0L).withRate(options.getQps(), Duration.standardSeconds(1L)))
                .apply("GenerateMessages", ParDo.of(new MessageGeneratorFn(options.getSchemaLocation(), options.getAttributeSchemaLocation())))
                .apply("WriteToPubsub", PubsubIO.writeMessages().to(options.getTopic()));

        return pipeline.run();
    }

    /**
     * The {@link MessageGeneratorFn} class generates {@link PubsubMessage} objects from a supplied
     * schema and populating the message with fake data.
     *
     * <p>See <a href="https://github.com/vincentrussell/json-data-generator">json-data-generator</a>
     * for instructions on how to construct the schema file.
     */
    static class MessageGeneratorFn extends DoFn<Long, PubsubMessage> {


        private static final Logger LOG = LoggerFactory.getLogger(MessageGeneratorFn.class);
        private final String schemaLocation;
        private String attributesSchemaLocation = null;
        private String schema;
        private String attributeSchema;

        // Not initialized inline or constructor because {@link JsonDataGenerator} is not serializable.
        private transient JsonDataGenerator dataGenerator;

        MessageGeneratorFn(String schemaLocation) {
            this.schemaLocation = schemaLocation;
        }

        MessageGeneratorFn(String schemaLocation, String attributesSchemaLocation) {
            this.schemaLocation = schemaLocation;
            this.attributesSchemaLocation = attributesSchemaLocation;
        }

        @Setup
        public void setup() throws IOException {
            dataGenerator = new JsonDataGeneratorImpl();

            Metadata metadata = FileSystems.matchSingleFileSpec(schemaLocation);

            // Copy the schema file into a string which can be used for generation.
            try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
                try (ReadableByteChannel readerChannel = FileSystems.open(metadata.resourceId())) {
                    try (WritableByteChannel writerChannel = Channels.newChannel(byteArrayOutputStream)) {
                        ByteStreams.copy(readerChannel, writerChannel);
                    }
                }

                schema = byteArrayOutputStream.toString();
            }

            if (attributesSchemaLocation != null) {
                Metadata metadataAttr = FileSystems.matchSingleFileSpec(attributesSchemaLocation);
                // Copy the schema file into a string which can be used for generation.
                try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
                    try (ReadableByteChannel readerChannel = FileSystems.open(metadataAttr.resourceId())) {
                        try (WritableByteChannel writerChannel = Channels.newChannel(byteArrayOutputStream)) {
                            ByteStreams.copy(readerChannel, writerChannel);
                        }
                    }
                    attributeSchema = byteArrayOutputStream.toString();
                }
            }
            //Set Timezone to UTC to generate datetime with UTC
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        }

        @ProcessElement
        public void processElement(@Element Long element, @Timestamp Instant timestamp,
                                   OutputReceiver<PubsubMessage> receiver, ProcessContext context)
                throws IOException, JsonDataGeneratorException {

            byte[] payload;
            byte[] attributesBytes;
            Map<String, String> attributes = new HashMap<>();

            // Generate the fake JSON according to the schema.
            try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
                dataGenerator.generateTestDataJson(schema, byteArrayOutputStream);
                payload = byteArrayOutputStream.toByteArray();
            }

            // Ability to place eventId and eventTimestamp in the attributes
            if (attributesSchemaLocation != null) {
                // Generate the fake JSON Attributes according to schema_attributes
                try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
                    dataGenerator.generateTestDataJson(attributeSchema, byteArrayOutputStream);
                    attributesBytes = byteArrayOutputStream.toByteArray();
                    String attr = new String(attributesBytes);
                    attributes = new ObjectMapper().readValue(attr, HashMap.class);
                }
            }
            receiver.output(new PubsubMessage(payload, attributes));
        }
    }


}
