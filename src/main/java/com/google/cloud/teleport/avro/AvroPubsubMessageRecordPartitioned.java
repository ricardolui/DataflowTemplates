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


package com.google.cloud.teleport.avro;

import com.google.common.base.MoreObjects;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.reflect.AvroSchema;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

/**
 * The {@link AvroPubsubMessageRecordPartitioned} class is an Avro wrapper class for {@link
 * org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage} which captures the message's fields along with
 * the event timestamp for archival purposes.
 */
@DefaultCoder(AvroCoder.class)
public class AvroPubsubMessageRecordPartitioned {


    private String originalSubscription;
    private byte[] message;
    private Map<String, String> attributes;

    @AvroSchema("{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}")
    private long timestamp;

    private String messageId;


    // Private empty constructor used for reflection required by AvroIO.
    @SuppressWarnings("unused")
    private AvroPubsubMessageRecordPartitioned() {
    }

    public AvroPubsubMessageRecordPartitioned(byte[] message, Map<String, String> attributes, long timestamp, String originalSubscription, String messageId) {
        this.message = message;
        this.attributes = attributes;
        this.timestamp = timestamp;
        this.originalSubscription = originalSubscription;
        this.messageId = messageId;
    }

    public AvroPubsubMessageRecordPartitioned(byte[] message, Map<String, String> attributes, long timestamp) {
        this.message = message;
        this.attributes = attributes;
        this.timestamp = timestamp;
    }


    public byte[] getMessage() {
        return this.message;
    }

    public void setMessage(byte[] message) {
        this.message = message;
    }

    public Map<String, String> getAttributes() {
        return this.attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }


    public String getOriginalSubscription() {
        return originalSubscription;
    }

    public void setOriginalSubscription(String originalSubscription) {
        this.originalSubscription = originalSubscription;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }


    @Override
    public boolean equals(Object obj) {

        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        final AvroPubsubMessageRecordPartitioned other = (AvroPubsubMessageRecordPartitioned) obj;

        return Objects.deepEquals(this.getMessage(), other.getMessage())
                && Objects.equals(this.getAttributes(), other.getAttributes())
                && Objects.equals(this.getTimestamp(), other.getTimestamp())
                && Objects.equals(this.getMessageId(), other.getMessageId())
                && Objects.equals(this.getOriginalSubscription(), other.getOriginalSubscription());
    }

    @Override
    public int hashCode() {
        return Objects.hash(message, attributes, timestamp, originalSubscription, messageId);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("message", message)
                .add("attributes", attributes)
                .add("timestamp", timestamp)
                .add("originalSubscription", originalSubscription)
                .add("messageId", messageId)
                .toString();
    }
}
