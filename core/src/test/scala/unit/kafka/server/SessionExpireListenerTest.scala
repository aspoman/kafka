/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  *    http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka.server

import kafka.api.ApiVersion
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.zookeeper.Watcher
import org.easymock.EasyMock
<<<<<<< HEAD
import org.junit.{Assert, Test}
import Assert._
import com.yammer.metrics.Metrics
import com.yammer.metrics.core.Meter
=======
import org.junit.{Assert, Before, Test}
import Assert._
import com.yammer.metrics.Metrics
import com.yammer.metrics.core.{Meter, Metric, MetricName}
import org.apache.kafka.common.utils.MockTime
>>>>>>> 065899a3bc330618e420673acf9504d123b800f3
import scala.collection.JavaConverters._

class SessionExpireListenerTest {

<<<<<<< HEAD
  private val brokerId = 1

  private def cleanMetricsRegistry() {
    val metrics = Metrics.defaultRegistry
    metrics.allMetrics.keySet.asScala.foreach(metrics.removeMetric)
  }

  @Test
  def testSessionExpireListenerMetrics() {

    cleanMetricsRegistry()

=======
  private var time = new MockTime
  private val brokerId = 1

  @Test
  def testSessionExpireListenerMetrics() {

>>>>>>> 065899a3bc330618e420673acf9504d123b800f3
    val metrics = Metrics.defaultRegistry

    def checkMeterCount(name: String, expected: Long) {
      val meter = metrics.allMetrics.asScala.collectFirst {
        case (metricName, meter: Meter) if metricName.getName == name => meter
      }.getOrElse(sys.error(s"Unable to find meter with name $name"))
<<<<<<< HEAD
      assertEquals(s"Unexpected meter count for $name", expected, meter.count)
=======
      assertEquals("Unexpected meter count", expected, meter.count)
>>>>>>> 065899a3bc330618e420673acf9504d123b800f3
    }

    val zkClient = EasyMock.mock(classOf[ZkClient])
    val zkUtils = ZkUtils(zkClient, isZkSecurityEnabled = false)
    import Watcher._
<<<<<<< HEAD
    val healthcheck = new KafkaHealthcheck(brokerId, Seq.empty, zkUtils, None, ApiVersion.latestVersion)
=======
    val healthcheck = new KafkaHealthcheck(brokerId, Map.empty, zkUtils, None, ApiVersion.latestVersion)
>>>>>>> 065899a3bc330618e420673acf9504d123b800f3

    val expiresPerSecName = "ZooKeeperExpiresPerSec"
    val disconnectsPerSecName = "ZooKeeperDisconnectsPerSec"
    checkMeterCount(expiresPerSecName, 0)
    checkMeterCount(disconnectsPerSecName, 0)

    healthcheck.sessionExpireListener.handleStateChanged(Event.KeeperState.Expired)
    checkMeterCount(expiresPerSecName, 1)
    checkMeterCount(disconnectsPerSecName, 0)

    healthcheck.sessionExpireListener.handleStateChanged(Event.KeeperState.Disconnected)
    checkMeterCount(expiresPerSecName, 1)
    checkMeterCount(disconnectsPerSecName, 1)
  }

}
