/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
<<<<<<< HEAD
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.hamcrest.CoreMatchers;
=======
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
>>>>>>> 065899a3bc330618e420673acf9504d123b800f3
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
<<<<<<< HEAD
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.common.requests.IsolationLevel.READ_COMMITTED;
import static org.apache.kafka.common.requests.IsolationLevel.READ_UNCOMMITTED;
import static org.apache.kafka.streams.StreamsConfig.EXACTLY_ONCE;
import static org.apache.kafka.streams.StreamsConfig.consumerPrefix;
import static org.apache.kafka.streams.StreamsConfig.producerPrefix;
import static org.apache.kafka.test.StreamsTestUtils.minimalStreamsConfig;
import static org.hamcrest.core.IsEqual.equalTo;
=======
import java.util.Map;
import java.util.Properties;

>>>>>>> 065899a3bc330618e420673acf9504d123b800f3
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StreamsConfigTest {

    private final Properties props = new Properties();
    private StreamsConfig streamsConfig;

    @Before
    public void setUp() {
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-config-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
<<<<<<< HEAD
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put("DUMMY", "dummy");
=======
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
>>>>>>> 065899a3bc330618e420673acf9504d123b800f3
        props.put("key.deserializer.encoding", "UTF8");
        props.put("value.deserializer.encoding", "UTF-16");
        streamsConfig = new StreamsConfig(props);
    }

    @Test(expected = ConfigException.class)
    public void shouldThrowExceptionIfApplicationIdIsNotSet() {
        props.remove(StreamsConfig.APPLICATION_ID_CONFIG);
        new StreamsConfig(props);
    }

    @Test(expected = ConfigException.class)
    public void shouldThrowExceptionIfBootstrapServersIsNotSet() {
        props.remove(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
        new StreamsConfig(props);
    }

    @Test
    public void testGetProducerConfigs() throws Exception {
        final String clientId = "client";
        final Map<String, Object> returnedProps = streamsConfig.getProducerConfigs(clientId);
        assertEquals(returnedProps.get(ProducerConfig.CLIENT_ID_CONFIG), clientId + "-producer");
        assertEquals(returnedProps.get(ProducerConfig.LINGER_MS_CONFIG), "100");
        assertNull(returnedProps.get("DUMMY"));
    }

    @Test
    public void testGetConsumerConfigs() throws Exception {
<<<<<<< HEAD
        final String groupId = "example-application";
        final String clientId = "client";
        final Map<String, Object> returnedProps = streamsConfig.getConsumerConfigs(null, groupId, clientId);
        assertEquals(returnedProps.get(ConsumerConfig.CLIENT_ID_CONFIG), clientId + "-consumer");
        assertEquals(returnedProps.get(ConsumerConfig.GROUP_ID_CONFIG), groupId);
        assertEquals(returnedProps.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), "1000");
        assertNull(returnedProps.get("DUMMY"));
=======
        Map<String, Object> returnedProps = streamsConfig.getConsumerConfigs(null, "example-application", "client");
        assertEquals(returnedProps.get(ConsumerConfig.CLIENT_ID_CONFIG), "client-consumer");
        assertEquals(returnedProps.get(ConsumerConfig.GROUP_ID_CONFIG), "example-application");

>>>>>>> 065899a3bc330618e420673acf9504d123b800f3
    }

    @Test
    public void testGetRestoreConsumerConfigs() throws Exception {
        final String clientId = "client";
        final Map<String, Object> returnedProps = streamsConfig.getRestoreConsumerConfigs(clientId);
        assertEquals(returnedProps.get(ConsumerConfig.CLIENT_ID_CONFIG), clientId + "-restore-consumer");
        assertNull(returnedProps.get(ConsumerConfig.GROUP_ID_CONFIG));
        assertNull(returnedProps.get("DUMMY"));
    }

    @Test
    public void defaultSerdeShouldBeConfigured() {
        final Map<String, Object> serializerConfigs = new HashMap<>();
        serializerConfigs.put("key.serializer.encoding", "UTF8");
        serializerConfigs.put("value.serializer.encoding", "UTF-16");
        final Serializer<String> serializer = Serdes.String().serializer();

        final String str = "my string for testing";
        final String topic = "my topic";

        serializer.configure(serializerConfigs, true);
        assertEquals("Should get the original string after serialization and deserialization with the configured encoding",
                str, streamsConfig.defaultKeySerde().deserializer().deserialize(topic, serializer.serialize(topic, str)));

        serializer.configure(serializerConfigs, false);
        assertEquals("Should get the original string after serialization and deserialization with the configured encoding",
                str, streamsConfig.defaultValueSerde().deserializer().deserialize(topic, serializer.serialize(topic, str)));
    }

    @Test
    public void shouldSupportMultipleBootstrapServers() {
        final List<String> expectedBootstrapServers = Arrays.asList("broker1:9092", "broker2:9092");
        final String bootstrapServersString = Utils.join(expectedBootstrapServers, ",");
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "irrelevant");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersString);
        final StreamsConfig config = new StreamsConfig(props);

        final List<String> actualBootstrapServers = config.getList(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
        assertEquals(expectedBootstrapServers, actualBootstrapServers);
    }

    @Test
    public void shouldSupportPrefixedConsumerConfigs() throws Exception {
        props.put(consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        props.put(consumerPrefix(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG), 1);
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getConsumerConfigs(null, "groupId", "clientId");
        assertEquals("earliest", consumerConfigs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertEquals(1, consumerConfigs.get(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG));
    }

    @Test
    public void shouldSupportPrefixedRestoreConsumerConfigs() throws Exception {
        props.put(consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        props.put(consumerPrefix(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG), 1);
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getRestoreConsumerConfigs("clientId");
        assertEquals("earliest", consumerConfigs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertEquals(1, consumerConfigs.get(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG));
    }

    @Test
    public void shouldSupportPrefixedPropertiesThatAreNotPartOfConsumerConfig() throws Exception {
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        props.put(consumerPrefix("interceptor.statsd.host"), "host");
        final Map<String, Object> consumerConfigs = streamsConfig.getConsumerConfigs(null, "groupId", "clientId");
        assertEquals("host", consumerConfigs.get("interceptor.statsd.host"));
    }

    @Test
    public void shouldSupportPrefixedPropertiesThatAreNotPartOfRestoreConsumerConfig() throws Exception {
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        props.put(consumerPrefix("interceptor.statsd.host"), "host");
        final Map<String, Object> consumerConfigs = streamsConfig.getRestoreConsumerConfigs("clientId");
        assertEquals("host", consumerConfigs.get("interceptor.statsd.host"));
    }

    @Test
    public void shouldSupportPrefixedPropertiesThatAreNotPartOfProducerConfig() throws Exception {
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        props.put(producerPrefix("interceptor.statsd.host"), "host");
        final Map<String, Object> producerConfigs = streamsConfig.getProducerConfigs("clientId");
        assertEquals("host", producerConfigs.get("interceptor.statsd.host"));
    }


    @Test
    public void shouldSupportPrefixedProducerConfigs() throws Exception {
        props.put(producerPrefix(ProducerConfig.BUFFER_MEMORY_CONFIG), 10);
        props.put(producerPrefix(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG), 1);
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> configs = streamsConfig.getProducerConfigs("clientId");
        assertEquals(10, configs.get(ProducerConfig.BUFFER_MEMORY_CONFIG));
        assertEquals(1, configs.get(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG));
    }

    @Test
    public void shouldBeSupportNonPrefixedConsumerConfigs() throws Exception {
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG, 1);
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getConsumerConfigs(null, "groupId", "clientId");
        assertEquals("earliest", consumerConfigs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertEquals(1, consumerConfigs.get(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG));
    }

    @Test
    public void shouldBeSupportNonPrefixedRestoreConsumerConfigs() throws Exception {
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG, 1);
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getRestoreConsumerConfigs("groupId");
        assertEquals("earliest", consumerConfigs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertEquals(1, consumerConfigs.get(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG));
    }

    @Test
    public void shouldSupportNonPrefixedProducerConfigs() throws Exception {
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 10);
        props.put(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG, 1);
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> configs = streamsConfig.getProducerConfigs("clientId");
        assertEquals(10, configs.get(ProducerConfig.BUFFER_MEMORY_CONFIG));
        assertEquals(1, configs.get(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG));
    }



    @Test(expected = StreamsException.class)
    public void shouldThrowStreamsExceptionIfKeySerdeConfigFails() throws Exception {
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, MisconfiguredSerde.class);
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        streamsConfig.defaultKeySerde();
    }

    @Test(expected = StreamsException.class)
    public void shouldThrowStreamsExceptionIfValueSerdeConfigFails() throws Exception {
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MisconfiguredSerde.class);
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        streamsConfig.defaultValueSerde();
    }

    @Test
    public void shouldOverrideStreamsDefaultConsumerConfigs() throws Exception {
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "latest");
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), "10");
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getConsumerConfigs(null, "groupId", "clientId");
        assertEquals("latest", consumerConfigs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertEquals("10", consumerConfigs.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
    }

    @Test
    public void shouldOverrideStreamsDefaultProducerConfigs() throws Exception {
        props.put(StreamsConfig.producerPrefix(ProducerConfig.LINGER_MS_CONFIG), "10000");
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> producerConfigs = streamsConfig.getProducerConfigs("clientId");
        assertEquals("10000", producerConfigs.get(ProducerConfig.LINGER_MS_CONFIG));
    }

    @Test
    public void shouldOverrideStreamsDefaultConsumerConifgsOnRestoreConsumer() throws Exception {
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "latest");
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), "10");
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getRestoreConsumerConfigs("clientId");
        assertEquals("latest", consumerConfigs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertEquals("10", consumerConfigs.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
    }

    @Test
    public void shouldResetToDefaultIfConsumerAutoCommitIsOverridden() {
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG), "true");
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getConsumerConfigs(null, "a", "b");
        assertEquals("false", consumerConfigs.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
    }

    @Test
    public void shouldResetToDefaultIfRestoreConsumerAutoCommitIsOverridden() {
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG), "true");
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getRestoreConsumerConfigs("client");
        assertEquals("false", consumerConfigs.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
    }

    @Test
    public void shouldSetInternalLeaveGroupOnCloseConfigToFalseInConsumer() throws Exception {
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getConsumerConfigs(null, "groupId", "clientId");
        assertThat(consumerConfigs.get("internal.leave.group.on.close"), CoreMatchers.<Object>equalTo(false));
    }

    @Test
    public void shouldAcceptAtLeastOnce() {
        // don't use `StreamsConfig.AT_LEAST_ONCE` to actually do a useful test
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "at_least_once");
        new StreamsConfig(props);
    }

    @Test
    public void shouldAcceptExactlyOnce() {
        // don't use `StreamsConfig.EXACLTY_ONCE` to actually do a useful test
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");
        new StreamsConfig(props);
    }

    @Test(expected = ConfigException.class)
    public void shouldThrowExceptionIfNotAtLestOnceOrExactlyOnce() {
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "bad_value");
        new StreamsConfig(props);
    }

    @Test
    public void shouldResetToDefaultIfConsumerIsolationLevelIsOverriddenIfEosEnabled() {
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "anyValue");
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getConsumerConfigs(null, "groupId", "clientId");
        assertThat((String) consumerConfigs.get(ConsumerConfig.ISOLATION_LEVEL_CONFIG), equalTo(READ_COMMITTED.name().toLowerCase(Locale.ROOT)));
    }

    @Test
    public void shouldAllowSettingConsumerIsolationLevelIfEosDisabled() {
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, READ_UNCOMMITTED.name().toLowerCase(Locale.ROOT));
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getConsumerConfigs(null, "groupId", "clientrId");
        assertThat((String) consumerConfigs.get(ConsumerConfig.ISOLATION_LEVEL_CONFIG), equalTo(READ_UNCOMMITTED.name().toLowerCase(Locale.ROOT)));
    }


    @Test
    public void shouldResetToDefaultIfProducerEnableIdempotenceIsOverriddenIfEosEnabled() {
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "anyValue");
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> producerConfigs = streamsConfig.getProducerConfigs("clientId");
        assertTrue((Boolean) producerConfigs.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
    }

    @Test
    public void shouldAllowSettingProducerEnableIdempotenceIfEosDisabled() {
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> producerConfigs = streamsConfig.getProducerConfigs("clientId");
        assertThat((Boolean) producerConfigs.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG), equalTo(false));
    }

    @Test
    public void shouldResetToDefaultIfProducerMaxInFlightRequestPerConnectionsIsOverriddenIfEosEnabled() {
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "anyValue");
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> producerConfigs = streamsConfig.getProducerConfigs("clientId");
        assertThat((Integer) producerConfigs.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION), equalTo(1));
    }

    @Test
    public void shouldAllowSettingProducerMaxInFlightRequestPerConnectionsWhenEosDisabled() {
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 2);
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> producerConfigs = streamsConfig.getProducerConfigs("clientId");
        assertThat((Integer) producerConfigs.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION), equalTo(2));
    }

    @Test
    public void shouldSetDifferentDefaultsIfEosEnabled() {
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE);
        final StreamsConfig streamsConfig = new StreamsConfig(props);

        final Map<String, Object> consumerConfigs = streamsConfig.getConsumerConfigs(null, "groupId", "clientId");
        final Map<String, Object> producerConfigs = streamsConfig.getProducerConfigs("clientId");

        assertThat((String) consumerConfigs.get(ConsumerConfig.ISOLATION_LEVEL_CONFIG), equalTo(READ_COMMITTED.name().toLowerCase(Locale.ROOT)));
        assertTrue((Boolean) producerConfigs.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        assertThat((Integer) producerConfigs.get(ProducerConfig.RETRIES_CONFIG), equalTo(Integer.MAX_VALUE));
        assertThat((Integer) producerConfigs.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION), equalTo(1));
        assertThat(streamsConfig.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG), equalTo(100L));
    }

    @Test
    public void shouldNotOverrideUserConfigRetriesIfExactlyOnceEnabled() {
        final int numberOfRetries = 42;
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE);
        props.put(ProducerConfig.RETRIES_CONFIG, numberOfRetries);
        final StreamsConfig streamsConfig = new StreamsConfig(props);

        final Map<String, Object> producerConfigs = streamsConfig.getProducerConfigs("clientId");

        assertThat((Integer) producerConfigs.get(ProducerConfig.RETRIES_CONFIG), equalTo(numberOfRetries));
    }

    @Test
    public void shouldNotOverrideUserConfigCommitIntervalMsIfExactlyOnceEnabled() {
        final long commitIntervalMs = 73L;
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, commitIntervalMs);
        final StreamsConfig streamsConfig = new StreamsConfig(props);

        assertThat(streamsConfig.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG), equalTo(commitIntervalMs));
    }

    @Test
    public void shouldBeBackwardsCompatibleWithDeprecatedConfigs() {
        final Properties props = minimalStreamsConfig();
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.Double().getClass());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.Double().getClass());
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MockTimestampExtractor.class);

        final StreamsConfig config = new StreamsConfig(props);
        assertTrue(config.defaultKeySerde() instanceof Serdes.DoubleSerde);
        assertTrue(config.defaultValueSerde() instanceof Serdes.DoubleSerde);
        assertTrue(config.defaultTimestampExtractor() instanceof MockTimestampExtractor);
    }

    @Test
    public void shouldUseNewConfigsWhenPresent() {
        final Properties props = minimalStreamsConfig();
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MockTimestampExtractor.class);

        final StreamsConfig config = new StreamsConfig(props);
        assertTrue(config.defaultKeySerde() instanceof Serdes.LongSerde);
        assertTrue(config.defaultValueSerde() instanceof Serdes.LongSerde);
        assertTrue(config.defaultTimestampExtractor() instanceof MockTimestampExtractor);
    }

    @Test
    public void shouldUseCorrectDefaultsWhenNoneSpecified() {
        final StreamsConfig config = new StreamsConfig(minimalStreamsConfig());
        assertTrue(config.defaultKeySerde() instanceof Serdes.ByteArraySerde);
        assertTrue(config.defaultValueSerde() instanceof Serdes.ByteArraySerde);
        assertTrue(config.defaultTimestampExtractor() instanceof FailOnInvalidTimestamp);
    }

    @Test
    public void shouldSpecifyCorrectKeySerdeClassOnErrorUsingDeprecatedConfigs() {
        final Properties props = minimalStreamsConfig();
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, MisconfiguredSerde.class);
        final StreamsConfig config = new StreamsConfig(props);
        try {
            config.keySerde();
            fail("Test should throw a StreamsException");
        } catch (StreamsException e) {
            assertEquals("Failed to configure key serde class org.apache.kafka.streams.StreamsConfigTest$MisconfiguredSerde", e.getMessage());
        }
    }

    @Test
    public void shouldSpecifyCorrectKeySerdeClassOnError() {
        final Properties props = minimalStreamsConfig();
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, MisconfiguredSerde.class);
        final StreamsConfig config = new StreamsConfig(props);
        try {
            config.keySerde();
            fail("Test should throw a StreamsException");
        } catch (StreamsException e) {
            assertEquals("Failed to configure key serde class org.apache.kafka.streams.StreamsConfigTest$MisconfiguredSerde", e.getMessage());
        }
    }

    @Test
    public void shouldSpecifyCorrectValueSerdeClassOnErrorUsingDeprecatedConfigs() {
        final Properties props = minimalStreamsConfig();
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, MisconfiguredSerde.class);
        final StreamsConfig config = new StreamsConfig(props);
        try {
            config.valueSerde();
            fail("Test should throw a StreamsException");
        } catch (StreamsException e) {
            assertEquals("Failed to configure value serde class org.apache.kafka.streams.StreamsConfigTest$MisconfiguredSerde", e.getMessage());
        }
    }

    @Test
    public void shouldSpecifyCorrectValueSerdeClassOnError() {
        final Properties props = minimalStreamsConfig();
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MisconfiguredSerde.class);
        final StreamsConfig config = new StreamsConfig(props);
        try {
            config.valueSerde();
            fail("Test should throw a StreamsException");
        } catch (StreamsException e) {
            assertEquals("Failed to configure value serde class org.apache.kafka.streams.StreamsConfigTest$MisconfiguredSerde", e.getMessage());
        }
    }



    static class MisconfiguredSerde implements Serde {
        @Override
        public void configure(final Map configs, final boolean isKey) {
            throw new RuntimeException("boom");
        }

        @Override
        public void close() {

        }

        @Override
        public Serializer serializer() {
            return null;
        }

        @Override
        public Deserializer deserializer() {
            return null;
        }
    }

    public static class MockTimestampExtractor implements TimestampExtractor {

        @Override
        public long extract(final ConsumerRecord<Object, Object> record, final long previousTimestamp) {
            return 0;
        }
    }

    @Test
    public void defaultSerdeShouldBeConfigured() {
        Map<String, Object> serializerConfigs = new HashMap<String, Object>();
        serializerConfigs.put("key.serializer.encoding", "UTF8");
        serializerConfigs.put("value.serializer.encoding", "UTF-16");
        Serializer<String> serializer = Serdes.String().serializer();

        String str = "my string for testing";
        String topic = "my topic";

        serializer.configure(serializerConfigs, true);
        assertEquals("Should get the original string after serialization and deserialization with the configured encoding",
                str, streamsConfig.keySerde().deserializer().deserialize(topic, serializer.serialize(topic, str)));

        serializer.configure(serializerConfigs, false);
        assertEquals("Should get the original string after serialization and deserialization with the configured encoding",
                str, streamsConfig.valueSerde().deserializer().deserialize(topic, serializer.serialize(topic, str)));
    }

    @Test
    public void shouldSupportMultipleBootstrapServers() {
        List<String> expectedBootstrapServers = Arrays.asList("broker1:9092", "broker2:9092");
        String bootstrapServersString = Utils.mkString(expectedBootstrapServers, ",").toString();
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "irrelevant");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersString);
        StreamsConfig config = new StreamsConfig(props);

        List<String> actualBootstrapServers = config.getList(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
        assertEquals(expectedBootstrapServers, actualBootstrapServers);
    }

}
