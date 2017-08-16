<<<<<<< HEAD
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
=======
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
>>>>>>> 065899a3bc330618e420673acf9504d123b800f3
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.integration;

<<<<<<< HEAD
import kafka.utils.MockTime;
=======

>>>>>>> 065899a3bc330618e420673acf9504d123b800f3
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
<<<<<<< HEAD
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.test.IntegrationTest;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

=======
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
>>>>>>> 065899a3bc330618e420673acf9504d123b800f3
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

<<<<<<< HEAD
=======
import org.apache.kafka.streams.integration.utils.EmbeddedSingleNodeKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;

>>>>>>> 065899a3bc330618e420673acf9504d123b800f3
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * End-to-end integration test that demonstrates "fan-out", using an embedded Kafka cluster.
<<<<<<< HEAD
 * <p>
 * This example shows how you can read from one input topic/stream, transform the data (here:
 * trivially) in two different ways via two intermediate streams, and then write the respective
 * results to two output topics.
 * <p>
=======
 *
 * This example shows how you can read from one input topic/stream, transform the data (here:
 * trivially) in two different ways via two intermediate streams, and then write the respective
 * results to two output topics.
 *
>>>>>>> 065899a3bc330618e420673acf9504d123b800f3
 * <pre>
 * {@code
 *
 *                                         +---map()---> stream2 ---to()---> Kafka topic B
 *                                         |
 * Kafka topic A ---stream()--> stream1 ---+
 *                                         |
 *                                         +---map()---> stream3 ---to()---> Kafka topic C
 *
 * }
 * </pre>
 */
<<<<<<< HEAD
@Category({IntegrationTest.class})
public class FanoutIntegrationTest {
    private static final int NUM_BROKERS = 1;
    private static final long COMMIT_INTERVAL_MS = 300L;

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final MockTime mockTime = CLUSTER.time;
=======
public class FanoutIntegrationTest {
    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();
>>>>>>> 065899a3bc330618e420673acf9504d123b800f3
    private static final String INPUT_TOPIC_A = "A";
    private static final String OUTPUT_TOPIC_B = "B";
    private static final String OUTPUT_TOPIC_C = "C";

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
<<<<<<< HEAD
        CLUSTER.createTopics(INPUT_TOPIC_A, OUTPUT_TOPIC_B, OUTPUT_TOPIC_C);
=======
        CLUSTER.createTopic(INPUT_TOPIC_A);
        CLUSTER.createTopic(OUTPUT_TOPIC_B);
        CLUSTER.createTopic(OUTPUT_TOPIC_C);
>>>>>>> 065899a3bc330618e420673acf9504d123b800f3
    }

    @Test
    public void shouldFanoutTheInput() throws Exception {
<<<<<<< HEAD
        final List<String> inputValues = Arrays.asList("Hello", "World");
        final List<String> expectedValuesForB = new ArrayList<>();
        final List<String> expectedValuesForC = new ArrayList<>();
        for (final String input : inputValues) {
=======
        List<String> inputValues = Arrays.asList("Hello", "World");
        List<String> expectedValuesForB = new ArrayList<>();
        List<String> expectedValuesForC = new ArrayList<>();
        for (String input : inputValues) {
>>>>>>> 065899a3bc330618e420673acf9504d123b800f3
            expectedValuesForB.add(input.toUpperCase(Locale.getDefault()));
            expectedValuesForC.add(input.toLowerCase(Locale.getDefault()));
        }

        //
        // Step 1: Configure and start the processor topology.
        //
<<<<<<< HEAD
        final StreamsBuilder builder = new StreamsBuilder();

        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "fanout-integration-test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL_MS);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KStream<byte[], String> stream1 = builder.stream(INPUT_TOPIC_A);
        final KStream<byte[], String> stream2 = stream1.mapValues(
            new ValueMapper<String, String>() {
                @Override
                public String apply(final String value) {
                    return value.toUpperCase(Locale.getDefault());
                }
            });
        final KStream<byte[], String> stream3 = stream1.mapValues(
            new ValueMapper<String, String>() {
                @Override
                public String apply(final String value) {
=======
        KStreamBuilder builder = new KStreamBuilder();

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "fanout-integration-test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, CLUSTER.zKConnectString());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        KStream<byte[], String> stream1 = builder.stream(INPUT_TOPIC_A);
        KStream<byte[], String> stream2 = stream1.mapValues(
            new ValueMapper<String, String>() {
                @Override
                public String apply(String value) {
                    return value.toUpperCase(Locale.getDefault());
                }
            });
        KStream<byte[], String> stream3 = stream1.mapValues(
            new ValueMapper<String, String>() {
                @Override
                public String apply(String value) {
>>>>>>> 065899a3bc330618e420673acf9504d123b800f3
                    return value.toLowerCase(Locale.getDefault());
                }
            });
        stream2.to(OUTPUT_TOPIC_B);
        stream3.to(OUTPUT_TOPIC_C);

<<<<<<< HEAD
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();

        //
        // Step 2: Produce some input data to the input topic.
        //
        final Properties producerConfig = new Properties();
=======
        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();

        // Wait briefly for the topology to be fully up and running (otherwise it might miss some or all
        // of the input data we produce below).
        Thread.sleep(5000);

        //
        // Step 2: Produce some input data to the input topic.
        //
        Properties producerConfig = new Properties();
>>>>>>> 065899a3bc330618e420673acf9504d123b800f3
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
<<<<<<< HEAD
        IntegrationTestUtils.produceValuesSynchronously(INPUT_TOPIC_A, inputValues, producerConfig, mockTime);
=======
        IntegrationTestUtils.produceValuesSynchronously(INPUT_TOPIC_A, inputValues, producerConfig);

        // Give the stream processing application some time to do its work.
        Thread.sleep(10000);
        streams.close();
>>>>>>> 065899a3bc330618e420673acf9504d123b800f3

        //
        // Step 3: Verify the application's output data.
        //

        // Verify output topic B
<<<<<<< HEAD
        final Properties consumerConfigB = new Properties();
=======
        Properties consumerConfigB = new Properties();
>>>>>>> 065899a3bc330618e420673acf9504d123b800f3
        consumerConfigB.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigB.put(ConsumerConfig.GROUP_ID_CONFIG, "fanout-integration-test-standard-consumer-topicB");
        consumerConfigB.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfigB.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerConfigB.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
<<<<<<< HEAD
        final List<String> actualValuesForB = IntegrationTestUtils.waitUntilMinValuesRecordsReceived(consumerConfigB,
            OUTPUT_TOPIC_B, inputValues.size());
        assertThat(actualValuesForB, equalTo(expectedValuesForB));

        // Verify output topic C
        final Properties consumerConfigC = new Properties();
=======
        List<String> actualValuesForB = IntegrationTestUtils.readValues(OUTPUT_TOPIC_B, consumerConfigB, inputValues.size());
        assertThat(actualValuesForB, equalTo(expectedValuesForB));

        // Verify output topic C
        Properties consumerConfigC = new Properties();
>>>>>>> 065899a3bc330618e420673acf9504d123b800f3
        consumerConfigC.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfigC.put(ConsumerConfig.GROUP_ID_CONFIG, "fanout-integration-test-standard-consumer-topicC");
        consumerConfigC.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfigC.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerConfigC.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
<<<<<<< HEAD
        final List<String> actualValuesForC = IntegrationTestUtils.waitUntilMinValuesRecordsReceived(consumerConfigC,
            OUTPUT_TOPIC_C, inputValues.size());
        streams.close();
        assertThat(actualValuesForC, equalTo(expectedValuesForC));
    }
}
=======
        List<String> actualValuesForC = IntegrationTestUtils.readValues(OUTPUT_TOPIC_C, consumerConfigC, inputValues.size());
        assertThat(actualValuesForC, equalTo(expectedValuesForC));
    }

}
>>>>>>> 065899a3bc330618e420673acf9504d123b800f3
