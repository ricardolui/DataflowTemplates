/*
 * Copyright (C) 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates;

import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.templates.options.RabbitMqToPubsubOptions;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test cases for the {@link RabbitMqToPubsub} class. */
@RunWith(JUnit4.class)
public class RabbitMqToPubsubTest {
  static final byte[][] BYTE_ARRAY = new byte[][] {"hi there".getBytes(StandardCharsets.UTF_8)};
  static final List<byte[]> WORDS = Arrays.asList(BYTE_ARRAY);
  @Rule public final transient TestPipeline p = TestPipeline.create();
  @Rule public ExpectedException exception = ExpectedException.none();
  static final String[] RESULT = new String[] {"hi there"};

  // @Test
  // public void testPipelineTransform() {
  //   PCollection<byte[]> input = p.apply(Create.of(WORDS));
  //   PCollection<String> output =
  //       input.apply("transform", ParDo.of(new RabbitMqToPubsub.ByteToStringTransform()));
  //   PAssert.that(output).containsInAnyOrder(RESULT);
  //   p.run();
  // }

  @Test
  public void testValidationFail() {
    RabbitMqToPubsubOptions options =
        PipelineOptionsFactory.create().as(RabbitMqToPubsubOptions.class);
    options.setConnectionUrl("amqp://rabbit:passwordrabbit22@localhost:5672");
    options.setOutputTopic("projects/project/topics/topic");
    assertThrows(IllegalArgumentException.class, () -> RabbitMqToPubsub.validate(options));
  }

  @Test
  public void testValidationSuccess() {
    RabbitMqToPubsubOptions options =
        PipelineOptionsFactory.create().as(RabbitMqToPubsubOptions.class);
    options.setConnectionUrl("amqp://rabbit:passwordrabbit22@hostname:15672");
    options.setQueue("queueTest");
    options.setOutputTopic("projects/project/topics/topic");

    // Expected to not throw an exception:
    RabbitMqToPubsub.validate(options);
  }
}
