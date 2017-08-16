<<<<<<< HEAD
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
package org.apache.kafka.streams.integration;

=======
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.kafka.streams.integration;


>>>>>>> 065899a3bc330618e420673acf9504d123b800f3
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
<<<<<<< HEAD
=======
import org.apache.kafka.common.serialization.Serde;
>>>>>>> 065899a3bc330618e420673acf9504d123b800f3
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
<<<<<<< HEAD
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * Tests all available joins of Kafka Streams DSL.
 */
@Category({IntegrationTest.class})
public class JoinIntegrationTest {
    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    private static final String APP_ID = "join-integration-test";
    private static final String INPUT_TOPIC_1 = "inputTopicLeft";
    private static final String INPUT_TOPIC_2 = "inputTopicRight";
    private static final String OUTPUT_TOPIC = "outputTopic";

    private final static Properties PRODUCER_CONFIG = new Properties();
    private final static Properties RESULT_CONSUMER_CONFIG = new Properties();
    private final static Properties STREAMS_CONFIG = new Properties();

    private StreamsBuilder builder;
    private KStream<Long, String> leftStream;
    private KStream<Long, String> rightStream;
    private KTable<Long, String> leftTable;
    private KTable<Long, String> rightTable;

    private final List<Input<String>> input = Arrays.asList(
        new Input<>(INPUT_TOPIC_1, (String) null),
        new Input<>(INPUT_TOPIC_2, (String) null),
        new Input<>(INPUT_TOPIC_1, "A"),
        new Input<>(INPUT_TOPIC_2, "a"),
        new Input<>(INPUT_TOPIC_1, "B"),
        new Input<>(INPUT_TOPIC_2, "b"),
        new Input<>(INPUT_TOPIC_1, (String) null),
        new Input<>(INPUT_TOPIC_2, (String) null),
        new Input<>(INPUT_TOPIC_1, "C"),
        new Input<>(INPUT_TOPIC_2, "c"),
        new Input<>(INPUT_TOPIC_2, (String) null),
        new Input<>(INPUT_TOPIC_1, (String) null),
        new Input<>(INPUT_TOPIC_2, (String) null),
        new Input<>(INPUT_TOPIC_2, "d"),
        new Input<>(INPUT_TOPIC_1, "D")
    );

    private final ValueJoiner<String, String, String> valueJoiner = new ValueJoiner<String, String, String>() {
        @Override
        public String apply(final String value1, final String value2) {
            return value1 + "-" + value2;
        }
    };

    @BeforeClass
    public static void setupConfigsAndUtils() throws Exception {
        PRODUCER_CONFIG.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        PRODUCER_CONFIG.put(ProducerConfig.ACKS_CONFIG, "all");
        PRODUCER_CONFIG.put(ProducerConfig.RETRIES_CONFIG, 0);
        PRODUCER_CONFIG.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        PRODUCER_CONFIG.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        RESULT_CONSUMER_CONFIG.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        RESULT_CONSUMER_CONFIG.put(ConsumerConfig.GROUP_ID_CONFIG, APP_ID + "-result-consumer");
        RESULT_CONSUMER_CONFIG.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        RESULT_CONSUMER_CONFIG.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        RESULT_CONSUMER_CONFIG.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        STREAMS_CONFIG.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        STREAMS_CONFIG.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        STREAMS_CONFIG.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        STREAMS_CONFIG.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        STREAMS_CONFIG.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        STREAMS_CONFIG.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        STREAMS_CONFIG.put(IntegrationTestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE, true);
        STREAMS_CONFIG.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
    }

    @Before
    public void prepareTopology() throws Exception {
        CLUSTER.createTopics(INPUT_TOPIC_1, INPUT_TOPIC_2, OUTPUT_TOPIC);

        builder = new StreamsBuilder();
        leftTable = builder.table(INPUT_TOPIC_1, "leftTable");
        rightTable = builder.table(INPUT_TOPIC_2, "rightTable");
        leftStream = leftTable.toStream();
        rightStream = rightTable.toStream();
    }

    @After
    public void cleanup() throws Exception {
        CLUSTER.deleteTopicsAndWait(120000, INPUT_TOPIC_1, INPUT_TOPIC_2, OUTPUT_TOPIC);
    }

    private void checkResult(final String outputTopic, final List<String> expectedResult) throws Exception {
        if (expectedResult != null) {
            final List<String> result = IntegrationTestUtils.waitUntilMinValuesRecordsReceived(RESULT_CONSUMER_CONFIG, outputTopic, expectedResult.size(), 30 * 1000L);
            assertThat(result, is(expectedResult));
        }
    }

    /*
     * Runs the actual test. Checks the result after each input record to ensure fixed processing order.
     * If an input tuple does not trigger any result, "expectedResult" should contain a "null" entry
     */
    private void runTest(final List<List<String>> expectedResult) throws Exception {
        assert expectedResult.size() == input.size();

        IntegrationTestUtils.purgeLocalStreamsState(STREAMS_CONFIG);
        final KafkaStreams streams = new KafkaStreams(builder.build(), STREAMS_CONFIG);
        try {
            streams.start();

            long ts = System.currentTimeMillis();

            final Iterator<List<String>> resultIterator = expectedResult.iterator();
            for (final Input<String> singleInput : input) {
                IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(singleInput.topic, Collections.singleton(singleInput.record), PRODUCER_CONFIG, ++ts);
                checkResult(OUTPUT_TOPIC, resultIterator.next());
            }
        } finally {
            streams.close();
        }
    }

    @Test
    public void testInnerKStreamKStream() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-inner-KStream-KStream");

        final List<List<String>> expectedResult = Arrays.asList(
            null,
            null,
            null,
            Collections.singletonList("A-a"),
            Collections.singletonList("B-a"),
            Arrays.asList("A-b", "B-b"),
            null,
            null,
            Arrays.asList("C-a", "C-b"),
            Arrays.asList("A-c", "B-c", "C-c"),
            null,
            null,
            null,
            Arrays.asList("A-d", "B-d", "C-d"),
            Arrays.asList("D-a", "D-b", "D-c", "D-d")
        );

        leftStream.join(rightStream, valueJoiner, JoinWindows.of(10000)).to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    @Test
    public void testLeftKStreamKStream() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-left-KStream-KStream");

        final List<List<String>> expectedResult = Arrays.asList(
            null,
            null,
            Collections.singletonList("A-null"),
            Collections.singletonList("A-a"),
            Collections.singletonList("B-a"),
            Arrays.asList("A-b", "B-b"),
            null,
            null,
            Arrays.asList("C-a", "C-b"),
            Arrays.asList("A-c", "B-c", "C-c"),
            null,
            null,
            null,
            Arrays.asList("A-d", "B-d", "C-d"),
            Arrays.asList("D-a", "D-b", "D-c", "D-d")
        );

        leftStream.leftJoin(rightStream, valueJoiner, JoinWindows.of(10000)).to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    @Test
    public void testOuterKStreamKStream() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-outer-KStream-KStream");

        final List<List<String>> expectedResult = Arrays.asList(
            null,
            null,
            Collections.singletonList("A-null"),
            Collections.singletonList("A-a"),
            Collections.singletonList("B-a"),
            Arrays.asList("A-b", "B-b"),
            null,
            null,
            Arrays.asList("C-a", "C-b"),
            Arrays.asList("A-c", "B-c", "C-c"),
            null,
            null,
            null,
            Arrays.asList("A-d", "B-d", "C-d"),
            Arrays.asList("D-a", "D-b", "D-c", "D-d")
        );

        leftStream.outerJoin(rightStream, valueJoiner, JoinWindows.of(10000)).to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    @Test
    public void testInnerKStreamKTable() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-inner-KStream-KTable");

        final List<List<String>> expectedResult = Arrays.asList(
            null,
            null,
            null,
            null,
            Collections.singletonList("B-a"),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            Collections.singletonList("D-d")
        );

        leftStream.join(rightTable, valueJoiner).to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    @Test
    public void testLeftKStreamKTable() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-left-KStream-KTable");

        final List<List<String>> expectedResult = Arrays.asList(
            null,
            null,
            Collections.singletonList("A-null"),
            null,
            Collections.singletonList("B-a"),
            null,
            null,
            null,
            Collections.singletonList("C-null"),
            null,
            null,
            null,
            null,
            null,
            Collections.singletonList("D-d")
        );

        leftStream.leftJoin(rightTable, valueJoiner).to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    @Test
    public void testInnerKTableKTable() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-inner-KTable-KTable");

        final List<List<String>> expectedResult = Arrays.asList(
            null,
            null,
            null,
            Collections.singletonList("A-a"),
            Collections.singletonList("B-a"),
            Collections.singletonList("B-b"),
            Collections.singletonList((String) null),
            null,
            null,
            Collections.singletonList("C-c"),
            Collections.singletonList((String) null),
            null,
            null,
            null,
            Collections.singletonList("D-d")
        );

        leftTable.join(rightTable, valueJoiner).to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    @Test
    public void testLeftKTableKTable() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-left-KTable-KTable");

        final List<List<String>> expectedResult = Arrays.asList(
            null,
            null,
            Collections.singletonList("A-null"),
            Collections.singletonList("A-a"),
            Collections.singletonList("B-a"),
            Collections.singletonList("B-b"),
            Collections.singletonList((String) null),
            null,
            Collections.singletonList("C-null"),
            Collections.singletonList("C-c"),
            Collections.singletonList("C-null"),
            Collections.singletonList((String) null),
            null,
            null,
            Collections.singletonList("D-d")
        );

        leftTable.leftJoin(rightTable, valueJoiner).to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    @Test
    public void testOuterKTableKTable() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID + "-outer-KTable-KTable");

        final List<List<String>> expectedResult = Arrays.asList(
            null,
            null,
            Collections.singletonList("A-null"),
            Collections.singletonList("A-a"),
            Collections.singletonList("B-a"),
            Collections.singletonList("B-b"),
            Collections.singletonList("null-b"),
            Collections.singletonList((String) null),
            Collections.singletonList("C-null"),
            Collections.singletonList("C-c"),
            Collections.singletonList("C-null"),
            Collections.singletonList((String) null),
            null,
            Collections.singletonList("null-d"),
            Collections.singletonList("D-d")
        );

        leftTable.outerJoin(rightTable, valueJoiner).to(OUTPUT_TOPIC);

        runTest(expectedResult);
    }

    private final class Input<V> {
        String topic;
        KeyValue<Long, V> record;

        private final long anyUniqueKey = 0L;

        Input(final String topic, final V value) {
            this.topic = topic;
            record = KeyValue.pair(anyUniqueKey, value);
        }
    }
}
=======
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.streams.integration.utils.EmbeddedSingleNodeKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * End-to-end integration test that demonstrates how to perform a join between a KStream and a
 * KTable (think: KStream.leftJoin(KTable)), i.e. an example of a stateful computation.
 */
public class JoinIntegrationTest {
    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();
    private static final String USER_CLICKS_TOPIC = "user-clicks";
    private static final String USER_REGIONS_TOPIC = "user-regions";
    private static final String OUTPUT_TOPIC = "output-topic";

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(USER_CLICKS_TOPIC);
        CLUSTER.createTopic(USER_REGIONS_TOPIC);
        CLUSTER.createTopic(OUTPUT_TOPIC);
    }

    /**
     * Tuple for a region and its associated number of clicks.
     */
    private static final class RegionWithClicks {

        private final String region;
        private final long clicks;

        public RegionWithClicks(String region, long clicks) {
            if (region == null || region.isEmpty()) {
                throw new IllegalArgumentException("region must be set");
            }
            if (clicks < 0) {
                throw new IllegalArgumentException("clicks must not be negative");
            }
            this.region = region;
            this.clicks = clicks;
        }

        public String getRegion() {
            return region;
        }

        public long getClicks() {
            return clicks;
        }

    }

    @Test
    public void shouldCountClicksPerRegion() throws Exception {
        // Input 1: Clicks per user (multiple records allowed per user).
        List<KeyValue<String, Long>> userClicks = Arrays.asList(
            new KeyValue<>("alice", 13L),
            new KeyValue<>("bob", 4L),
            new KeyValue<>("chao", 25L),
            new KeyValue<>("bob", 19L),
            new KeyValue<>("dave", 56L),
            new KeyValue<>("eve", 78L),
            new KeyValue<>("alice", 40L),
            new KeyValue<>("fang", 99L)
        );

        // Input 2: Region per user (multiple records allowed per user).
        List<KeyValue<String, String>> userRegions = Arrays.asList(
            new KeyValue<>("alice", "asia"),   /* Alice lived in Asia originally... */
            new KeyValue<>("bob", "americas"),
            new KeyValue<>("chao", "asia"),
            new KeyValue<>("dave", "europe"),
            new KeyValue<>("alice", "europe"), /* ...but moved to Europe some time later. */
            new KeyValue<>("eve", "americas"),
            new KeyValue<>("fang", "asia")
        );

        List<KeyValue<String, Long>> expectedClicksPerRegion = Arrays.asList(
            new KeyValue<>("europe", 13L),
            new KeyValue<>("americas", 4L),
            new KeyValue<>("asia", 25L),
            new KeyValue<>("americas", 23L),
            new KeyValue<>("europe", 69L),
            new KeyValue<>("americas", 101L),
            new KeyValue<>("europe", 109L),
            new KeyValue<>("asia", 124L)
        );

        //
        // Step 1: Configure and start the processor topology.
        //
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "join-integration-test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, CLUSTER.zKConnectString());
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        // Explicitly place the state directory under /tmp so that we can remove it via
        // `purgeLocalStreamsState` below.  Once Streams is updated to expose the effective
        // StreamsConfig configuration (so we can retrieve whatever state directory Streams came up
        // with automatically) we don't need to set this anymore and can update `purgeLocalStreamsState`
        // accordingly.
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");

        // Remove any state from previous test runs
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);

        KStreamBuilder builder = new KStreamBuilder();

        // This KStream contains information such as "alice" -> 13L.
        //
        // Because this is a KStream ("record stream"), multiple records for the same user will be
        // considered as separate click-count events, each of which will be added to the total count.
        KStream<String, Long> userClicksStream = builder.stream(stringSerde, longSerde, USER_CLICKS_TOPIC);

        // This KTable contains information such as "alice" -> "europe".
        //
        // Because this is a KTable ("changelog stream"), only the latest value (here: region) for a
        // record key will be considered at the time when a new user-click record (see above) is
        // received for the `leftJoin` below.  Any previous region values are being considered out of
        // date.  This behavior is quite different to the KStream for user clicks above.
        //
        // For example, the user "alice" will be considered to live in "europe" (although originally she
        // lived in "asia") because, at the time her first user-click record is being received and
        // subsequently processed in the `leftJoin`, the latest region update for "alice" is "europe"
        // (which overrides her previous region value of "asia").
        KTable<String, String> userRegionsTable =
            builder.table(stringSerde, stringSerde, USER_REGIONS_TOPIC);

        // Compute the number of clicks per region, e.g. "europe" -> 13L.
        //
        // The resulting KTable is continuously being updated as new data records are arriving in the
        // input KStream `userClicksStream` and input KTable `userRegionsTable`.
        KTable<String, Long> clicksPerRegion = userClicksStream
            // Join the stream against the table.
            //
            // Null values possible: In general, null values are possible for region (i.e. the value of
            // the KTable we are joining against) so we must guard against that (here: by setting the
            // fallback region "UNKNOWN").  In this specific example this is not really needed because
            // we know, based on the test setup, that all users have appropriate region entries at the
            // time we perform the join.
            //
            // Also, we need to return a tuple of (region, clicks) for each user.  But because Java does
            // not support tuples out-of-the-box, we must use a custom class `RegionWithClicks` to
            // achieve the same effect.
            .leftJoin(userRegionsTable, new ValueJoiner<Long, String, RegionWithClicks>() {
                @Override
                public RegionWithClicks apply(Long clicks, String region) {
                    RegionWithClicks regionWithClicks = new RegionWithClicks(region == null ? "UNKNOWN" : region, clicks);
                    return regionWithClicks;
                }
            })
            // Change the stream from <user> -> <region, clicks> to <region> -> <clicks>
            .map(new KeyValueMapper<String, RegionWithClicks, KeyValue<String, Long>>() {
                @Override
                public KeyValue<String, Long> apply(String key, RegionWithClicks value) {
                    return new KeyValue<>(value.getRegion(), value.getClicks());
                }
            })
            // Compute the total per region by summing the individual click counts per region.
            .reduceByKey(new Reducer<Long>() {
                @Override
                public Long apply(Long value1, Long value2) {
                    return value1 + value2;
                }
            }, stringSerde, longSerde, "ClicksPerRegionUnwindowed");

        // Write the (continuously updating) results to the output topic.
        clicksPerRegion.to(stringSerde, longSerde, OUTPUT_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();

        // Wait briefly for the topology to be fully up and running (otherwise it might miss some or all
        // of the input data we produce below).
        Thread.sleep(5000);

        //
        // Step 2: Publish user-region information.
        //
        // To keep this code example simple and easier to understand/reason about, we publish all
        // user-region records before any user-click records (cf. step 3).  In practice though,
        // data records would typically be arriving concurrently in both input streams/topics.
        Properties userRegionsProducerConfig = new Properties();
        userRegionsProducerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        userRegionsProducerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        userRegionsProducerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        userRegionsProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        userRegionsProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        IntegrationTestUtils.produceKeyValuesSynchronously(USER_REGIONS_TOPIC, userRegions, userRegionsProducerConfig);

        //
        // Step 3: Publish some user click events.
        //
        Properties userClicksProducerConfig = new Properties();
        userClicksProducerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        userClicksProducerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        userClicksProducerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        userClicksProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        userClicksProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        IntegrationTestUtils.produceKeyValuesSynchronously(USER_CLICKS_TOPIC, userClicks, userClicksProducerConfig);

        // Give the stream processing application some time to do its work.
        Thread.sleep(10000);
        streams.close();

        //
        // Step 4: Verify the application's output data.
        //
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "join-integration-test-standard-consumer");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        List<KeyValue<String, Long>> actualClicksPerRegion = IntegrationTestUtils.readKeyValues(OUTPUT_TOPIC, consumerConfig);
        assertThat(actualClicksPerRegion, equalTo(expectedClicksPerRegion));
    }

}
>>>>>>> 065899a3bc330618e420673acf9504d123b800f3
