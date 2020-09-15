/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */


package com.google.cloud.teleport.templates;

import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.teleport.avro.AvroPubsubMessageRecordPartitioned;
import com.google.cloud.teleport.io.WindowedFilenamePolicy;
import com.google.cloud.teleport.util.DurationUtils;
import com.google.pubsub.v1.ProjectName;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.Topic;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This pipeline ingests incoming data from a Cloud Pub/Sub topic and outputs the raw data into
 * windowed Avro files at the specified output directory.
 *
 * <p>Files output will have the following schema:
 *
 * <pre>
 *   {
 *      "type": "record",
 *      "name": "AvroPubsubMessageRecord",
 *      "namespace": "com.google.cloud.teleport.avro",
 *      "fields": [
 *        {"name": "message", "type": {"type": "array", "items": "bytes"}},
 *        {"name": "attributes", "type": {"type": "map", "values": "string"}},
 *        {"name": "timestamp", "type": "long"}
 *      ]
 *   }
 * </pre>
 *
 * <p>Example Usage:
 *
 * <pre>
 * mvn compile exec:java \
 *   -Dexec.mainClass=com.google.cloud.teleport.templates.${PIPELINE_NAME} \
 *   -Dexec.cleanupDaemonThreads=false \
 *   -Dexec.args=" \
 *   --project=${PROJECT_ID} \
 *   --stagingLocation=gs://${PROJECT_ID}/dataflow/pipelines/${PIPELINE_FOLDER}/staging \
 *   --tempLocation=gs://${PROJECT_ID}/dataflow/pipelines/${PIPELINE_FOLDER}/temp \
 *   --runner=DataflowRunner \
 *   --windowDuration=2m \
 *   --numShards=1 \
 *   --readFromTopic=false \
 *   --derivationName=bkp \
 *   --outputDirectory=gs://${PROJECT_ID}/avro/backup/ \
 *   --outputFilenamePrefix=windowed-file \
 *   --outputFilenameSuffix=.avro
 *   --avroTempDirectory=gs://${PROJECT_ID}/avro-temp-dir/"
 * </pre>
 */
public class PubsubToAvroPartitioned {

    /**
     * The log to output status messages to.
     */
    private static final Logger LOG = LoggerFactory.getLogger(PubsubToAvroPartitioned.class);

    /**
     * Main entry point for executing the pipeline.
     *
     * @param args The command-line arguments to the pipeline.
     */
    public static void main(String[] args) {

        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        options.setStreaming(true);

        run(options);
    }

    public static List<String> listTopicOrSubscriptions(boolean isTopic, String currentProject, String derivedName) {

        List<String> listToRead = new ArrayList<String>();

        if (isTopic) {
            try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
                ProjectName projectName = ProjectName.of(currentProject);
                for (Topic topic :
                        topicAdminClient.listTopics(projectName).iterateAll()) {

                    LOG.debug(topic.getName());

                    if (topic.getName().contains(derivedName)) {
                        listToRead.add(topic.getName());
                        LOG.info("Added topic: " + topic.getName());
                    }
                }
                LOG.info("Listed all the subscriptions in the project.");
            } catch (IOException e) {
                LOG.error("Exception listing subscriptions in project" + e.getMessage());
                e.printStackTrace();
            }
        } else {
            try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
                ProjectName projectName = ProjectName.of(currentProject);
                for (Subscription subscription :
                        subscriptionAdminClient.listSubscriptions(projectName).iterateAll()) {

                    LOG.debug(subscription.getName());

                    if (subscription.getName().contains(derivedName)) {
                        listToRead.add(subscription.getName());
                        LOG.info("Added subs: " + subscription.getName());
                    }
                }
                LOG.info("Listed all the subscriptions in the project.");
            } catch (IOException e) {
                LOG.error("Exception listing subscriptions in project" + e.getMessage());
                e.printStackTrace();
            }
        }

        return listToRead;
    }

    /**
     * Runs the pipeline with the supplied options.
     *
     * @param options The execution parameters to the pipeline.
     * @return The result of the pipeline execution.
     */
    public static PipelineResult run(Options options) {
        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);

        List<String> listTopicOrSubs = listTopicOrSubscriptions(options.getReadFromTopic().get(), options.getProject(), options.getDerivationName().get());

        if (listTopicOrSubs.size() == 0) {
            LOG.error("No Topics or Subscriptions");
        }

        List<PCollection<KV<String, PubsubMessage>>> listPCollections = new ArrayList<PCollection<KV<String, PubsubMessage>>>();

        for (String subs : listTopicOrSubs) {

            PCollection<KV<String, PubsubMessage>> readSubs;
            if (options.getReadFromTopic().get()) {
                readSubs = pipeline.apply("ReadPubSubSubscription", PubsubIO.readMessagesWithAttributes().fromTopic(subs))
                        .apply("Map With Sub", WithKeys.of(input -> subs)).setCoder(KvCoder.of(StringUtf8Coder.of(), PubsubMessageWithAttributesCoder.of()));
            } else {
                readSubs = pipeline.apply("ReadPubSubSubscription", PubsubIO.readMessagesWithAttributes().fromSubscription(subs))
                        .apply("Map With Sub", WithKeys.of(input -> subs)).setCoder(KvCoder.of(StringUtf8Coder.of(), PubsubMessageWithAttributesCoder.of()));
            }

            listPCollections.add(readSubs);
        }


        PCollectionList.of(listPCollections).apply(Flatten.<KV<String, PubsubMessage>>pCollections())
                .apply("Map To Archive", ParDo.of(new PubsubMessageToKVArchiveDoFn()))
                .apply(options.getWindowDuration() + " Window", Window.into(FixedWindows.of(DurationUtils.parseDuration(options.getWindowDuration()))))
                .apply("Write File(s)",
                        AvroIO.write(AvroPubsubMessageRecordPartitioned.class)
                                .to(
                                        new WindowedFilenamePolicy(
                                                ValueProvider.StaticValueProvider.of(options.getOutputDirectory().get() + "year=YYYY/month=MM/day=DD/hour=HH/"),
                                                options.getOutputFilenamePrefix(),
                                                options.getOutputShardTemplate(),
                                                options.getOutputFilenameSuffix()))
                                .withTempDirectory(ValueProvider.NestedValueProvider.of(
                                        options.getAvroTempDirectory(),
                                        (SerializableFunction<String, ResourceId>) input ->
                                                FileBasedSink.convertToFileResourceIfPossible(input)))
                                /*.withTempDirectory(FileSystems.matchNewResource(
                                    options.getAvroTempDirectory(),
                                    Boolean.TRUE))
                                    */
                                .withWindowedWrites()
                                .withNumShards(options.getNumShards()));


        // Execute the pipeline and return the result.
        return pipeline.run();
    }


    /**
     * Options supported by the pipeline.
     *
     * <p>Inherits standard configuration options.
     */
    public interface Options extends PipelineOptions, StreamingOptions, GcpOptions {


        @Description("Read From Topic, if not, from subscription")
        @Default.Boolean(false)
        @Required
        ValueProvider<Boolean> getReadFromTopic();

        void setReadFromTopic(ValueProvider<Boolean> value);


        @Description("The directory to output files to. Must end with a slash.")
        @Required
        ValueProvider<String> getOutputDirectory();

        void setOutputDirectory(ValueProvider<String> value);

        @Description("The filename prefix of the files to write to.")
        @Default.String("output")
        ValueProvider<String> getOutputFilenamePrefix();

        void setOutputFilenamePrefix(ValueProvider<String> value);

        @Description("The suffix of the files to write.")
        @Default.String("")
        ValueProvider<String> getOutputFilenameSuffix();

        void setOutputFilenameSuffix(ValueProvider<String> value);

        @Description(
                "The shard template of the output file. Specified as repeating sequences "
                        + "of the letters 'S' or 'N' (example: SSS-NNN). These are replaced with the "
                        + "shard number, or number of shards respectively")
        @Default.String("W-P-SS-of-NN")
        ValueProvider<String> getOutputShardTemplate();

        void setOutputShardTemplate(ValueProvider<String> value);

        @Description("The maximum number of output shards produced when writing.")
        @Default.Integer(1)
        Integer getNumShards();

        void setNumShards(Integer value);

        @Description(
                "The window duration in which data will be written. Defaults to 5m. "
                        + "Allowed formats are: "
                        + "Ns (for seconds, example: 5s), "
                        + "Nm (for minutes, example: 12m), "
                        + "Nh (for hours, example: 2h).")
        @Default.String("3m")
        String getWindowDuration();

        void setWindowDuration(String value);

        @Description("The Avro Write Temporary Directory. Must end with /")
        @Required
        ValueProvider<String> getAvroTempDirectory();

        void setAvroTempDirectory(ValueProvider<String> value);


        @Description("String inside topic or subscription that contains the following word such as backup or bkp")
        ValueProvider<String> getDerivationName();

        void setDerivationName(ValueProvider<String> derivationName);


    }

    /**
     * Converts an incoming {@link PubsubMessage} to the {@link AvroPubsubMessageRecordPartitioned} class by
     * copying it's fields and the timestamp of the message.
     */
    static class PubsubMessageToKVArchiveDoFn extends DoFn<KV<String, PubsubMessage>, AvroPubsubMessageRecordPartitioned> {
        @ProcessElement
        public void processElement(ProcessContext context) {
            KV<String, PubsubMessage> el = context.element();
            PubsubMessage message = el.getValue();
            context.output(new AvroPubsubMessageRecordPartitioned(message.getPayload(), message.getAttributeMap(), context.timestamp().getMillis(), el.getKey(), message.getMessageId()));

        }
    }


}
