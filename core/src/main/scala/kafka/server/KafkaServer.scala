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

import java.io.{File, IOException}
import java.net.{InetAddress, SocketTimeoutException}
import java.util
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import kafka.api.{KAFKA_0_9_0, KAFKA_2_2_IV0, KAFKA_2_4_IV1}
import kafka.cluster.Broker
import kafka.common.{GenerateBrokerIdException, InconsistentBrokerIdException, InconsistentBrokerMetadataException, InconsistentClusterIdException}
import kafka.controller.KafkaController
import kafka.coordinator.group.GroupCoordinator
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.log.{LogConfig, LogManager}
import kafka.metrics.{KafkaMetricsGroup, KafkaMetricsReporter, KafkaYammerMetrics}
import kafka.network.SocketServer
import kafka.security.CredentialProvider
import kafka.utils._
import kafka.zk.{BrokerInfo, KafkaZkClient}
import org.apache.kafka.clients.{ApiVersions, ClientDnsLookup, ManualMetadataUpdater, NetworkClient, NetworkClientUtils, CommonClientConfigs}
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.message.ControlledShutdownRequestData
import org.apache.kafka.common.metrics.{JmxReporter, Metrics, MetricsReporter, _}
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{ControlledShutdownRequest, ControlledShutdownResponse}
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.security.{JaasContext, JaasUtils}
import org.apache.kafka.common.utils.{AppInfoParser, LogContext, Time}
import org.apache.kafka.common.{ClusterResource, Endpoint, Node}
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.zookeeper.client.ZKClientConfig

import scala.jdk.CollectionConverters._
import scala.collection.{Map, Seq, mutable}

object KafkaServer {
  // Copy the subset of properties that are relevant to Logs
  // I'm listing out individual properties here since the names are slightly different in each Config class...
  private[kafka] def copyKafkaConfigToLog(kafkaConfig: KafkaConfig): java.util.Map[String, Object] = {
    val logProps = new util.HashMap[String, Object]()
    logProps.put(LogConfig.SegmentBytesProp, kafkaConfig.logSegmentBytes)
    logProps.put(LogConfig.SegmentMsProp, kafkaConfig.logRollTimeMillis)
    logProps.put(LogConfig.SegmentJitterMsProp, kafkaConfig.logRollTimeJitterMillis)
    logProps.put(LogConfig.SegmentIndexBytesProp, kafkaConfig.logIndexSizeMaxBytes)
    logProps.put(LogConfig.FlushMessagesProp, kafkaConfig.logFlushIntervalMessages)
    logProps.put(LogConfig.FlushMsProp, kafkaConfig.logFlushIntervalMs)
    logProps.put(LogConfig.RetentionBytesProp, kafkaConfig.logRetentionBytes)
    logProps.put(LogConfig.RetentionMsProp, kafkaConfig.logRetentionTimeMillis: java.lang.Long)
    logProps.put(LogConfig.MaxMessageBytesProp, kafkaConfig.messageMaxBytes)
    logProps.put(LogConfig.IndexIntervalBytesProp, kafkaConfig.logIndexIntervalBytes)
    logProps.put(LogConfig.DeleteRetentionMsProp, kafkaConfig.logCleanerDeleteRetentionMs)
    logProps.put(LogConfig.MinCompactionLagMsProp, kafkaConfig.logCleanerMinCompactionLagMs)
    logProps.put(LogConfig.MaxCompactionLagMsProp, kafkaConfig.logCleanerMaxCompactionLagMs)
    logProps.put(LogConfig.FileDeleteDelayMsProp, kafkaConfig.logDeleteDelayMs)
    logProps.put(LogConfig.MinCleanableDirtyRatioProp, kafkaConfig.logCleanerMinCleanRatio)
    logProps.put(LogConfig.CleanupPolicyProp, kafkaConfig.logCleanupPolicy)
    logProps.put(LogConfig.MinInSyncReplicasProp, kafkaConfig.minInSyncReplicas)
    logProps.put(LogConfig.CompressionTypeProp, kafkaConfig.compressionType)
    logProps.put(LogConfig.UncleanLeaderElectionEnableProp, kafkaConfig.uncleanLeaderElectionEnable)
    logProps.put(LogConfig.PreAllocateEnableProp, kafkaConfig.logPreAllocateEnable)
    logProps.put(LogConfig.MessageFormatVersionProp, kafkaConfig.logMessageFormatVersion.version)
    logProps.put(LogConfig.MessageTimestampTypeProp, kafkaConfig.logMessageTimestampType.name)
    logProps.put(LogConfig.MessageTimestampDifferenceMaxMsProp, kafkaConfig.logMessageTimestampDifferenceMaxMs: java.lang.Long)
    logProps.put(LogConfig.MessageDownConversionEnableProp, kafkaConfig.logMessageDownConversionEnable: java.lang.Boolean)
    logProps
  }

  private[server] def metricConfig(kafkaConfig: KafkaConfig): MetricConfig = {
    new MetricConfig()
      .samples(kafkaConfig.metricNumSamples)
      .recordLevel(Sensor.RecordingLevel.forName(kafkaConfig.metricRecordingLevel))
      .timeWindow(kafkaConfig.metricSampleWindowMs, TimeUnit.MILLISECONDS)
  }

  def zkClientConfigFromKafkaConfig(config: KafkaConfig, forceZkSslClientEnable: Boolean = false) =
    if (!config.zkSslClientEnable && !forceZkSslClientEnable)
      None
    else {
      val clientConfig = new ZKClientConfig()
      KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslClientEnableProp, "true")
      config.zkClientCnxnSocketClassName.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkClientCnxnSocketProp, _))
      config.zkSslKeyStoreLocation.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslKeyStoreLocationProp, _))
      config.zkSslKeyStorePassword.foreach(x => KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslKeyStorePasswordProp, x.value))
      config.zkSslKeyStoreType.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslKeyStoreTypeProp, _))
      config.zkSslTrustStoreLocation.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslTrustStoreLocationProp, _))
      config.zkSslTrustStorePassword.foreach(x => KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslTrustStorePasswordProp, x.value))
      config.zkSslTrustStoreType.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslTrustStoreTypeProp, _))
      KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslProtocolProp, config.ZkSslProtocol)
      config.ZkSslEnabledProtocols.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslEnabledProtocolsProp, _))
      config.ZkSslCipherSuites.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslCipherSuitesProp, _))
      KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslEndpointIdentificationAlgorithmProp, config.ZkSslEndpointIdentificationAlgorithm)
      KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslCrlEnableProp, config.ZkSslCrlEnable.toString)
      KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslOcspEnableProp, config.ZkSslOcspEnable.toString)
      Some(clientConfig)
    }

  val MIN_INCREMENTAL_FETCH_SESSION_EVICTION_MS: Long = 120000
}

/**
 * Represents the lifecycle of a single Kafka broker. Handles all functionality required
 * to start up and shutdown a single Kafka node.
 */
class KafkaServer(val config: KafkaConfig, time: Time = Time.SYSTEM, threadNamePrefix: Option[String] = None,
                  kafkaMetricsReporters: Seq[KafkaMetricsReporter] = List()) extends Logging with KafkaMetricsGroup {
  private val startupComplete = new AtomicBoolean(false)
  private val isShuttingDown = new AtomicBoolean(false)
  private val isStartingUp = new AtomicBoolean(false)

  private var shutdownLatch = new CountDownLatch(1)

  //properties for MetricsContext
  private val metricsPrefix: String = "kafka.server"
  private val KAFKA_CLUSTER_ID: String = "kafka.cluster.id"
  private val KAFKA_BROKER_ID: String = "kafka.broker.id"


  private var logContext: LogContext = null

  var kafkaYammerMetrics: KafkaYammerMetrics = null
  var metrics: Metrics = null

  val brokerState: BrokerState = new BrokerState

  var dataPlaneRequestProcessor: KafkaApis = null
  var controlPlaneRequestProcessor: KafkaApis = null

  var authorizer: Option[Authorizer] = None
  var socketServer: SocketServer = null
  var dataPlaneRequestHandlerPool: KafkaRequestHandlerPool = null
  var controlPlaneRequestHandlerPool: KafkaRequestHandlerPool = null

  var logDirFailureChannel: LogDirFailureChannel = null
  var logManager: LogManager = null

  var replicaManager: ReplicaManager = null
  var adminManager: AdminManager = null
  var tokenManager: DelegationTokenManager = null

  var dynamicConfigHandlers: Map[String, ConfigHandler] = null
  var dynamicConfigManager: DynamicConfigManager = null
  var credentialProvider: CredentialProvider = null
  var tokenCache: DelegationTokenCache = null

  var groupCoordinator: GroupCoordinator = null

  var transactionCoordinator: TransactionCoordinator = null

  var kafkaController: KafkaController = null

  var brokerToControllerChannelManager: BrokerToControllerChannelManager = null

  var kafkaScheduler: KafkaScheduler = null

  var metadataCache: MetadataCache = null
  var quotaManagers: QuotaFactory.QuotaManagers = null

  val zkClientConfig: ZKClientConfig = KafkaServer.zkClientConfigFromKafkaConfig(config).getOrElse(new ZKClientConfig())
  private var _zkClient: KafkaZkClient = null

  val correlationId: AtomicInteger = new AtomicInteger(0)
  val brokerMetaPropsFile = "meta.properties"
  val brokerMetadataCheckpoints = config.logDirs.map(logDir => (logDir, new BrokerMetadataCheckpoint(new File(logDir + File.separator + brokerMetaPropsFile)))).toMap

  private var _clusterId: String = null
  private var _brokerTopicStats: BrokerTopicStats = null

  private var _featureChangeListener: FinalizedFeatureChangeListener = null

  val brokerFeatures: BrokerFeatures = BrokerFeatures.createDefault()

  val featureCache: FinalizedFeatureCache = new FinalizedFeatureCache(brokerFeatures)

  def clusterId: String = _clusterId

  // Visible for testing
  private[kafka] def zkClient = _zkClient

  private[kafka] def brokerTopicStats = _brokerTopicStats

  private[kafka] def featureChangeListener = _featureChangeListener

  newGauge("BrokerState", () => brokerState.currentState)
  newGauge("ClusterId", () => clusterId)
  newGauge("yammer-metrics-count", () =>  KafkaYammerMetrics.defaultRegistry.allMetrics.size)

  val linuxIoMetricsCollector = new LinuxIoMetricsCollector("/proc", time, logger.underlying)

  if (linuxIoMetricsCollector.usable()) {
    newGauge("linux-disk-read-bytes", () => linuxIoMetricsCollector.readBytes())
    newGauge("linux-disk-write-bytes", () => linuxIoMetricsCollector.writeBytes())
  }

  /**
   * Start up API for bringing up a single instance of the Kafka server.
   * Instantiates the LogManager, the SocketServer and the request handlers - KafkaRequestHandlers
   *
   * 启动Kafka服务器的单个实例。
   * 实例化LogManager，SocketServer和KafkaRequestHandlers，
   *
   * ·始化zk的连接 {@link initZkClient}
   * ·监听zk变化 {@link FinalizedFeatureChangeListener.initOrThrow} todo 未完
   * ·获取cluster_id {@link getOrGenerateClusterId}
   * ·加载metadata信息
   * ·初始化brokerId
   * ·启动一个线程池执行一些后台定时任务
   * ·创建metrics
   * ·启动日志管理
   * ·创建元数据缓存MetadataCache
   * ·启动socketServer，监听端口
   * ·启动副本管理
   * ·启动token管理器
   * ·启动kafka控制器
   * ·启动GroupCoordinator
   * ·启动事务机制TransactionCoordinator
   * ·启动认证与授权机制
   * ·创建“处理数据类请求”相关类dataPlaneRequestProcessor
   * ·开启动态配置监听
   * ·socketserver开始处理请求
   */
  def startup(): Unit = {
    try {
      info("starting")

      if (isShuttingDown.get)
        throw new IllegalStateException("Kafka server is still shutting down, cannot re-start!")

      if (startupComplete.get)
        return

      //此处采用cas判断是否可以启动，防止线程并发进行启动。
      val canStartup = isStartingUp.compareAndSet(false, true)

      //启动kafka broker
      if (canStartup) {
        //将状态码设置为Starting
        brokerState.newState(Starting)

        /* setup zookeeper */
        //初始化zk的连接
        initZkClient(time)

        /* initialize features */
        //监听zk变化（2.7版本加的新功能）
        _featureChangeListener = new FinalizedFeatureChangeListener(featureCache, _zkClient)
        if (config.isFeatureVersioningSupported) {
          // 只有大于等于2.7的版本才会开启此功能
          _featureChangeListener.initOrThrow(config.zkConnectionTimeoutMs)
        }

        /* Get or create cluster_id */
        //获取cluster_id，先从zk中获取，获取不到则创建
        _clusterId = getOrGenerateClusterId(zkClient)
        info(s"Cluster ID = $clusterId")

        /* load metadata */
        // 加载metadata，就是Topic/Partion与broker的映射关系，
        // metadata信息里包括了每个topic的所有partition的信息: leader, leader_epoch, controller_epoch, isr, replicas等;
        val (preloadedBrokerMetadataCheckpoint, initialOfflineDirs) = getBrokerMetadataAndOfflineDirs

        /* check cluster id */
        //校验本地的cluster id与zk上的是否相同
        if (preloadedBrokerMetadataCheckpoint.clusterId.isDefined && preloadedBrokerMetadataCheckpoint.clusterId.get != clusterId)
          throw new InconsistentClusterIdException(
            s"The Cluster ID ${clusterId} doesn't match stored clusterId ${preloadedBrokerMetadataCheckpoint.clusterId} in meta.properties. " +
            s"The broker is trying to join the wrong cluster. Configured zookeeper.connect may be wrong.")

        /* generate brokerId */
        //初始化brokerId，如果配置文件中有则使用配置文件中等，否则根据zk生成
        config.brokerId = getOrGenerateBrokerId(preloadedBrokerMetadataCheckpoint)
        logContext = new LogContext(s"[KafkaServer id=${config.brokerId}] ")
        this.logIdent = logContext.logPrefix

        // initialize dynamic broker configs from ZooKeeper. Any updates made after this will be
        // applied after DynamicConfigManager starts.
        // 实现本地配置与zk配置的同步
        config.dynamicConfig.initialize(zkClient)

        /* start scheduler */
        //启动一个线程池执行一些后台定时任务
        kafkaScheduler = new KafkaScheduler(config.backgroundThreads)
        kafkaScheduler.startup()

        /* create and configure metrics */
        //创建server的metrics监控
        kafkaYammerMetrics = KafkaYammerMetrics.INSTANCE
        kafkaYammerMetrics.configure(config.originals)

        // kafka使用Yammer Metrics来记录JMX数据。
        val jmxReporter = new JmxReporter()
        jmxReporter.configure(config.originals)

        val reporters = new util.ArrayList[MetricsReporter]
        reporters.add(jmxReporter)

        //创建metrics
        val metricConfig = KafkaServer.metricConfig(config)
        val metricsContext = createKafkaMetricsContext()
        metrics = new Metrics(metricConfig, reporters, time, true, metricsContext)

        /* register broker metrics */
        _brokerTopicStats = new BrokerTopicStats

        quotaManagers = QuotaFactory.instantiate(config, metrics, time, threadNamePrefix.getOrElse(""))
        notifyClusterListeners(kafkaMetricsReporters ++ metrics.reporters.asScala)

        logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size)

        /* start log manager */
        // 启动日志管理
        logManager = LogManager(config, initialOfflineDirs, zkClient, brokerState, kafkaScheduler, time, brokerTopicStats, logDirFailureChannel)
        logManager.startup()

        // MetadataCache指Broker上的元数据缓存
        metadataCache = new MetadataCache(config.brokerId)
        // Enable delegation token cache for all SCRAM mechanisms to simplify dynamic update.
        // This keeps the cache up-to-date if new SCRAM mechanisms are enabled dynamically.
        tokenCache = new DelegationTokenCache(ScramMechanism.mechanismNames)
        credentialProvider = new CredentialProvider(ScramMechanism.mechanismNames, tokenCache)

        // Create and start the socket server acceptor threads so that the bound port is known.
        // Delay starting processors until the end of the initialization sequence to ensure
        // that credentials have been loaded before processing authentications.
        // 启动socketServer，监听端口
        socketServer = new SocketServer(config, metrics, time, credentialProvider)
        socketServer.startup(startProcessingRequests = false)

        /* start replica manager */
        // 启动副本管理
        brokerToControllerChannelManager = new BrokerToControllerChannelManagerImpl(metadataCache, time, metrics, config, threadNamePrefix)
        replicaManager = createReplicaManager(isShuttingDown)
        replicaManager.startup()
        brokerToControllerChannelManager.start()

        // broker信息同步到zk，并进行一些检查
        val brokerInfo = createBrokerInfo
        val brokerEpoch = zkClient.registerBroker(brokerInfo)

        // Now that the broker is successfully registered, checkpoint its metadata
        checkpointBrokerMetadata(BrokerMetadata(config.brokerId, Some(clusterId)))

        /* start token manager */
        // 启动token管理器
        tokenManager = new DelegationTokenManager(config, tokenCache, time , zkClient)
        tokenManager.startup()

        /* start kafka controller */
        // 启动kafka控制器，kafkaController为集群中的所有主题分区选取领导者副本；另一方面，它还承载着集群的全部元数据信息，并负责讲这些元数据信息同步到其他broker上。
        kafkaController = new KafkaController(config, zkClient, time, metrics, brokerInfo, brokerEpoch, tokenManager, brokerFeatures, featureCache, threadNamePrefix)
        kafkaController.startup()

        adminManager = new AdminManager(config, metrics, metadataCache, zkClient)

        /* start group coordinator */
        // Hardcode Time.SYSTEM for now as some Streams tests fail otherwise, it would be good to fix the underlying issue
        // GroupCoordinator 是负责进行 consumer 的 group 成员的rebalance与 offset 管理。
        groupCoordinator = GroupCoordinator(config, zkClient, replicaManager, Time.SYSTEM, metrics)
        groupCoordinator.startup()

        /* start transaction coordinator, with a separate background thread scheduler for transaction expiration and log loading */
        // Hardcode Time.SYSTEM for now as some Streams tests fail otherwise, it would be good to fix the underlying issue
        // kafka的事务机制
        transactionCoordinator = TransactionCoordinator(config, replicaManager, new KafkaScheduler(threads = 1, threadNamePrefix = "transaction-log-manager-"), zkClient, metrics, metadataCache, Time.SYSTEM)
        transactionCoordinator.startup()

        /* Get the authorizer and initialize it if one is specified.*/
        // 认证与授权机制
        authorizer = config.authorizer
        authorizer.foreach(_.configure(config.originals))
        val authorizerFutures: Map[Endpoint, CompletableFuture[Void]] = authorizer match {
          case Some(authZ) =>
            authZ.start(brokerInfo.broker.toServerInfo(clusterId, config)).asScala.map { case (ep, cs) =>
              ep -> cs.toCompletableFuture
            }
          case None =>
            brokerInfo.broker.endPoints.map { ep =>
              ep.toJava -> CompletableFuture.completedFuture[Void](null)
            }.toMap
        }

        val fetchManager = new FetchManager(Time.SYSTEM,
          new FetchSessionCache(config.maxIncrementalFetchSessionCacheSlots,
            KafkaServer.MIN_INCREMENTAL_FETCH_SESSION_EVICTION_MS))

        /* start processing requests */
        // 创建“处理数据类请求”相关类dataPlaneRequestProcessor
        dataPlaneRequestProcessor = new KafkaApis(socketServer.dataPlaneRequestChannel, replicaManager, adminManager, groupCoordinator, transactionCoordinator,
          kafkaController, zkClient, config.brokerId, config, metadataCache, metrics, authorizer, quotaManagers,
          fetchManager, brokerTopicStats, clusterId, time, tokenManager, brokerFeatures, featureCache)

        dataPlaneRequestHandlerPool = new KafkaRequestHandlerPool(config.brokerId, socketServer.dataPlaneRequestChannel, dataPlaneRequestProcessor, time,
          config.numIoThreads, s"${SocketServer.DataPlaneMetricPrefix}RequestHandlerAvgIdlePercent", SocketServer.DataPlaneThreadPrefix)

        socketServer.controlPlaneRequestChannelOpt.foreach { controlPlaneRequestChannel =>
          controlPlaneRequestProcessor = new KafkaApis(controlPlaneRequestChannel, replicaManager, adminManager, groupCoordinator, transactionCoordinator,
            kafkaController, zkClient, config.brokerId, config, metadataCache, metrics, authorizer, quotaManagers,
            fetchManager, brokerTopicStats, clusterId, time, tokenManager, brokerFeatures, featureCache)

          controlPlaneRequestHandlerPool = new KafkaRequestHandlerPool(config.brokerId, socketServer.controlPlaneRequestChannelOpt.get, controlPlaneRequestProcessor, time,
            1, s"${SocketServer.ControlPlaneMetricPrefix}RequestHandlerAvgIdlePercent", SocketServer.ControlPlaneThreadPrefix)
        }

        Mx4jLoader.maybeLoad()

        /* Add all reconfigurables for config change notification before starting config handlers */
        config.dynamicConfig.addReconfigurables(this)

        /* start dynamic config manager */
        // kafka动态配置管理
        dynamicConfigHandlers = Map[String, ConfigHandler](ConfigType.Topic -> new TopicConfigHandler(logManager, config, quotaManagers, kafkaController),
                                                           ConfigType.Client -> new ClientIdConfigHandler(quotaManagers),
                                                           ConfigType.User -> new UserConfigHandler(quotaManagers, credentialProvider),
                                                           ConfigType.Broker -> new BrokerConfigHandler(config, quotaManagers))

        // Create the config manager. start listening to notifications
        //开始监听动态配置
        dynamicConfigManager = new DynamicConfigManager(zkClient, dynamicConfigHandlers)
        dynamicConfigManager.startup()

        // socketserver开始处理请求
        socketServer.startProcessingRequests(authorizerFutures)

        brokerState.newState(RunningAsBroker)
        shutdownLatch = new CountDownLatch(1)
        startupComplete.set(true)
        isStartingUp.set(false)
        AppInfoParser.registerAppInfo(metricsPrefix, config.brokerId.toString, metrics, time.milliseconds())
        info("started")
      }
    }
    catch {
      case e: Throwable =>
        fatal("Fatal error during KafkaServer startup. Prepare to shutdown", e)
        isStartingUp.set(false)
        shutdown()
        throw e
    }
  }

  private[server] def notifyClusterListeners(clusterListeners: Seq[AnyRef]): Unit = {
    val clusterResourceListeners = new ClusterResourceListeners
    clusterResourceListeners.maybeAddAll(clusterListeners.asJava)
    clusterResourceListeners.onUpdate(new ClusterResource(clusterId))
  }

  private[server] def notifyMetricsReporters(metricsReporters: Seq[AnyRef]): Unit = {
    val metricsContext = createKafkaMetricsContext()
    metricsReporters.foreach {
      case x: MetricsReporter => x.contextChange(metricsContext)
      case _ => //do nothing
    }
  }

  private[server] def createKafkaMetricsContext() : KafkaMetricsContext = {
    val contextLabels = new util.HashMap[String, Object]
    contextLabels.put(KAFKA_CLUSTER_ID, clusterId)
    contextLabels.put(KAFKA_BROKER_ID, config.brokerId.toString)
    contextLabels.putAll(config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX))
    val metricsContext = new KafkaMetricsContext(metricsPrefix, contextLabels)
    metricsContext
  }

  protected def createReplicaManager(isShuttingDown: AtomicBoolean): ReplicaManager = {
    val alterIsrManager = new AlterIsrManagerImpl(brokerToControllerChannelManager, kafkaScheduler,
      time, config.brokerId, () => kafkaController.brokerEpoch)
    new ReplicaManager(config, metrics, time, zkClient, kafkaScheduler, logManager, isShuttingDown, quotaManagers,
      brokerTopicStats, metadataCache, logDirFailureChannel, alterIsrManager)
  }

  /**
   * 该方法初始化并创建了KafkaZkClient对象
   *
   * points：
   * 1、可以指定zk的存储根目录，且不存在的话会提前创建
   * 2、可以设置zk的acl权限控制
   * 3、创建KafkaZkClient对象 {@link KafkaZkClient.apply}，而kafkaZkClient对象内部包含一个ZooKeeperClient对象
   * @param time
   */
  private def initZkClient(time: Time): Unit = {
    info(s"Connecting to zookeeper on ${config.zkConnect}")

    //创建KafkaZkClient对象
    def createZkClient(zkConnect: String, isSecure: Boolean) = {
      KafkaZkClient(zkConnect, isSecure, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs,
        config.zkMaxInFlightRequests, time, name = Some("Kafka server"), zkClientConfig = Some(zkClientConfig))
    }


    /**
     * kafka默认使用zk的根目录/，
     * 但可以在配置文件的zookeeper.connect字段结尾增加/路径
     * 而此处所做的就是判断配置文件中有没有重新指定根目录
     */
    val chrootIndex = config.zkConnect.indexOf("/") //返回配置文件zookeeper.connect字段中"/"符号第一次出现的位置
    val chrootOption = {
      if (chrootIndex > 0) Some(config.zkConnect.substring(chrootIndex))
      else None
    }

    // 是否设置zk的acl权限控制
    val secureAclsEnabled = config.zkEnableSecureAcls
    val isZkSecurityEnabled = JaasUtils.isZkSaslEnabled() || KafkaConfig.zkTlsClientAuthEnabled(zkClientConfig)

    if (secureAclsEnabled && !isZkSecurityEnabled)
      throw new java.lang.SecurityException(s"${KafkaConfig.ZkEnableSecureAclsProp} is true, but ZooKeeper client TLS configuration identifying at least $KafkaConfig.ZkSslClientEnableProp, $KafkaConfig.ZkClientCnxnSocketProp, and $KafkaConfig.ZkSslKeyStoreLocationProp was not present and the " +
        s"verification of the JAAS login file failed ${JaasUtils.zkSecuritySysConfigString}")

    // make sure chroot path exists
    // 如果设置了指定根目录，则此步骤中进行创建
    chrootOption.foreach { chroot =>
      val zkConnForChrootCreation = config.zkConnect.substring(0, chrootIndex)
      val zkClient = createZkClient(zkConnForChrootCreation, secureAclsEnabled)
      zkClient.makeSurePersistentPathExists(chroot)
      info(s"Created zookeeper path $chroot")
      zkClient.close()
    }

    // 创建KafkaZkClient对象
    _zkClient = createZkClient(config.zkConnect, secureAclsEnabled)
    // 在zk中预先创建顶级路径
    _zkClient.createTopLevelPaths()
  }

  private def getOrGenerateClusterId(zkClient: KafkaZkClient): String = {
    /**
     * 尝试通过zkclient从zk中获取，如果没有值，则通过zkclient创建
     */
    zkClient.getClusterId.getOrElse(zkClient.createOrGetClusterId(CoreUtils.generateUuidAsBase64()))
  }

  def createBrokerInfo: BrokerInfo = {
    val endPoints = config.advertisedListeners.map(e => s"${e.host}:${e.port}")
    zkClient.getAllBrokersInCluster.filter(_.id != config.brokerId).foreach { broker =>
      val commonEndPoints = broker.endPoints.map(e => s"${e.host}:${e.port}").intersect(endPoints)
      require(commonEndPoints.isEmpty, s"Configured end points ${commonEndPoints.mkString(",")} in" +
        s" advertised listeners are already registered by broker ${broker.id}")
    }

    val listeners = config.advertisedListeners.map { endpoint =>
      if (endpoint.port == 0)
        endpoint.copy(port = socketServer.boundPort(endpoint.listenerName))
      else
        endpoint
    }

    val updatedEndpoints = listeners.map(endpoint =>
      if (endpoint.host == null || endpoint.host.trim.isEmpty)
        endpoint.copy(host = InetAddress.getLocalHost.getCanonicalHostName)
      else
        endpoint
    )

    val jmxPort = System.getProperty("com.sun.management.jmxremote.port", "-1").toInt
    BrokerInfo(
      Broker(config.brokerId, updatedEndpoints, config.rack, brokerFeatures.supportedFeatures),
      config.interBrokerProtocolVersion,
      jmxPort)
  }

  /**
   * Performs controlled shutdown
   */
  private def controlledShutdown(): Unit = {

    def node(broker: Broker): Node = broker.node(config.interBrokerListenerName)

    val socketTimeoutMs = config.controllerSocketTimeoutMs

    def doControlledShutdown(retries: Int): Boolean = {
      val metadataUpdater = new ManualMetadataUpdater()
      val networkClient = {
        val channelBuilder = ChannelBuilders.clientChannelBuilder(
          config.interBrokerSecurityProtocol,
          JaasContext.Type.SERVER,
          config,
          config.interBrokerListenerName,
          config.saslMechanismInterBrokerProtocol,
          time,
          config.saslInterBrokerHandshakeRequestEnable,
          logContext)
        val selector = new Selector(
          NetworkReceive.UNLIMITED,
          config.connectionsMaxIdleMs,
          metrics,
          time,
          "kafka-server-controlled-shutdown",
          Map.empty.asJava,
          false,
          channelBuilder,
          logContext
        )
        new NetworkClient(
          selector,
          metadataUpdater,
          config.brokerId.toString,
          1,
          0,
          0,
          Selectable.USE_DEFAULT_BUFFER_SIZE,
          Selectable.USE_DEFAULT_BUFFER_SIZE,
          config.requestTimeoutMs,
          config.connectionSetupTimeoutMs,
          config.connectionSetupTimeoutMaxMs,
          ClientDnsLookup.USE_ALL_DNS_IPS,
          time,
          false,
          new ApiVersions,
          logContext)
      }

      var shutdownSucceeded: Boolean = false

      try {

        var remainingRetries = retries
        var prevController: Broker = null
        var ioException = false

        while (!shutdownSucceeded && remainingRetries > 0) {
          remainingRetries = remainingRetries - 1

          // 1. Find the controller and establish a connection to it.

          // Get the current controller info. This is to ensure we use the most recent info to issue the
          // controlled shutdown request.
          // If the controller id or the broker registration are missing, we sleep and retry (if there are remaining retries)
          zkClient.getControllerId match {
            case Some(controllerId) =>
              zkClient.getBroker(controllerId) match {
                case Some(broker) =>
                  // if this is the first attempt, if the controller has changed or if an exception was thrown in a previous
                  // attempt, connect to the most recent controller
                  if (ioException || broker != prevController) {

                    ioException = false

                    if (prevController != null)
                      networkClient.close(node(prevController).idString)

                    prevController = broker
                    metadataUpdater.setNodes(Seq(node(prevController)).asJava)
                  }
                case None =>
                  info(s"Broker registration for controller $controllerId is not available (i.e. the Controller's ZK session expired)")
              }
            case None =>
              info("No controller registered in ZooKeeper")
          }

          // 2. issue a controlled shutdown to the controller
          if (prevController != null) {
            try {

              if (!NetworkClientUtils.awaitReady(networkClient, node(prevController), time, socketTimeoutMs))
                throw new SocketTimeoutException(s"Failed to connect within $socketTimeoutMs ms")

              // send the controlled shutdown request
              val controlledShutdownApiVersion: Short =
                if (config.interBrokerProtocolVersion < KAFKA_0_9_0) 0
                else if (config.interBrokerProtocolVersion < KAFKA_2_2_IV0) 1
                else if (config.interBrokerProtocolVersion < KAFKA_2_4_IV1) 2
                else 3

              val controlledShutdownRequest = new ControlledShutdownRequest.Builder(
                  new ControlledShutdownRequestData()
                    .setBrokerId(config.brokerId)
                    .setBrokerEpoch(kafkaController.brokerEpoch),
                    controlledShutdownApiVersion)
              val request = networkClient.newClientRequest(node(prevController).idString, controlledShutdownRequest,
                time.milliseconds(), true)
              val clientResponse = NetworkClientUtils.sendAndReceive(networkClient, request, time)

              val shutdownResponse = clientResponse.responseBody.asInstanceOf[ControlledShutdownResponse]
              if (shutdownResponse.error == Errors.NONE && shutdownResponse.data.remainingPartitions.isEmpty) {
                shutdownSucceeded = true
                info("Controlled shutdown succeeded")
              }
              else {
                info(s"Remaining partitions to move: ${shutdownResponse.data.remainingPartitions}")
                info(s"Error from controller: ${shutdownResponse.error}")
              }
            }
            catch {
              case ioe: IOException =>
                ioException = true
                warn("Error during controlled shutdown, possibly because leader movement took longer than the " +
                  s"configured controller.socket.timeout.ms and/or request.timeout.ms: ${ioe.getMessage}")
                // ignore and try again
            }
          }
          if (!shutdownSucceeded) {
            Thread.sleep(config.controlledShutdownRetryBackoffMs)
            warn("Retrying controlled shutdown after the previous attempt failed...")
          }
        }
      }
      finally
        networkClient.close()

      shutdownSucceeded
    }

    if (startupComplete.get() && config.controlledShutdownEnable) {
      // We request the controller to do a controlled shutdown. On failure, we backoff for a configured period
      // of time and try again for a configured number of retries. If all the attempt fails, we simply force
      // the shutdown.
      info("Starting controlled shutdown")

      brokerState.newState(PendingControlledShutdown)

      val shutdownSucceeded = doControlledShutdown(config.controlledShutdownMaxRetries.intValue)

      if (!shutdownSucceeded)
        warn("Proceeding to do an unclean shutdown as all the controlled shutdown attempts failed")
    }
  }

  /**
   * Shutdown API for shutting down a single instance of the Kafka server.
   * Shuts down the LogManager, the SocketServer and the log cleaner scheduler thread
   */
  def shutdown(): Unit = {
    try {
      info("shutting down")

      if (isStartingUp.get)
        throw new IllegalStateException("Kafka server is still starting up, cannot shut down!")

      // To ensure correct behavior under concurrent calls, we need to check `shutdownLatch` first since it gets updated
      // last in the `if` block. If the order is reversed, we could shutdown twice or leave `isShuttingDown` set to
      // `true` at the end of this method.
      if (shutdownLatch.getCount > 0 && isShuttingDown.compareAndSet(false, true)) {
        CoreUtils.swallow(controlledShutdown(), this)
        brokerState.newState(BrokerShuttingDown)

        if (dynamicConfigManager != null)
          CoreUtils.swallow(dynamicConfigManager.shutdown(), this)

        // Stop socket server to stop accepting any more connections and requests.
        // Socket server will be shutdown towards the end of the sequence.
        if (socketServer != null)
          CoreUtils.swallow(socketServer.stopProcessingRequests(), this)
        if (dataPlaneRequestHandlerPool != null)
          CoreUtils.swallow(dataPlaneRequestHandlerPool.shutdown(), this)
        if (controlPlaneRequestHandlerPool != null)
          CoreUtils.swallow(controlPlaneRequestHandlerPool.shutdown(), this)
        if (kafkaScheduler != null)
          CoreUtils.swallow(kafkaScheduler.shutdown(), this)

        if (dataPlaneRequestProcessor != null)
          CoreUtils.swallow(dataPlaneRequestProcessor.close(), this)
        if (controlPlaneRequestProcessor != null)
          CoreUtils.swallow(controlPlaneRequestProcessor.close(), this)
        CoreUtils.swallow(authorizer.foreach(_.close()), this)
        if (adminManager != null)
          CoreUtils.swallow(adminManager.shutdown(), this)

        if (transactionCoordinator != null)
          CoreUtils.swallow(transactionCoordinator.shutdown(), this)
        if (groupCoordinator != null)
          CoreUtils.swallow(groupCoordinator.shutdown(), this)

        if (tokenManager != null)
          CoreUtils.swallow(tokenManager.shutdown(), this)

        if (replicaManager != null)
          CoreUtils.swallow(replicaManager.shutdown(), this)

        if (brokerToControllerChannelManager != null)
          CoreUtils.swallow(brokerToControllerChannelManager.shutdown(), this)

        if (logManager != null)
          CoreUtils.swallow(logManager.shutdown(), this)

        if (kafkaController != null)
          CoreUtils.swallow(kafkaController.shutdown(), this)

        if (featureChangeListener != null)
          CoreUtils.swallow(featureChangeListener.close(), this)

        if (zkClient != null)
          CoreUtils.swallow(zkClient.close(), this)

        if (quotaManagers != null)
          CoreUtils.swallow(quotaManagers.shutdown(), this)

        // Even though socket server is stopped much earlier, controller can generate
        // response for controlled shutdown request. Shutdown server at the end to
        // avoid any failures (e.g. when metrics are recorded)
        if (socketServer != null)
          CoreUtils.swallow(socketServer.shutdown(), this)
        if (metrics != null)
          CoreUtils.swallow(metrics.close(), this)
        if (brokerTopicStats != null)
          CoreUtils.swallow(brokerTopicStats.close(), this)

        // Clear all reconfigurable instances stored in DynamicBrokerConfig
        config.dynamicConfig.clear()

        brokerState.newState(NotRunning)

        startupComplete.set(false)
        isShuttingDown.set(false)
        CoreUtils.swallow(AppInfoParser.unregisterAppInfo(metricsPrefix, config.brokerId.toString, metrics), this)
        shutdownLatch.countDown()
        info("shut down completed")
      }
    }
    catch {
      case e: Throwable =>
        fatal("Fatal error during KafkaServer shutdown.", e)
        isShuttingDown.set(false)
        throw e
    }
  }

  /**
   * After calling shutdown(), use this API to wait until the shutdown is complete
   */
  def awaitShutdown(): Unit = shutdownLatch.await()

  def getLogManager: LogManager = logManager

  def boundPort(listenerName: ListenerName): Int = socketServer.boundPort(listenerName)

  /**
   * Reads the BrokerMetadata. If the BrokerMetadata doesn't match in all the log.dirs, InconsistentBrokerMetadataException is
   * thrown.
   *
   * The log directories whose meta.properties can not be accessed due to IOException will be returned to the caller
   *
   * @return A 2-tuple containing the brokerMetadata and a sequence of offline log directories.
   */
  private def getBrokerMetadataAndOfflineDirs: (BrokerMetadata, Seq[String]) = {
    val brokerMetadataMap = mutable.HashMap[String, BrokerMetadata]()
    val brokerMetadataSet = mutable.HashSet[BrokerMetadata]()
    val offlineDirs = mutable.ArrayBuffer.empty[String]

    for (logDir <- config.logDirs) {
      try {
        val brokerMetadataOpt = brokerMetadataCheckpoints(logDir).read()
        brokerMetadataOpt.foreach { brokerMetadata =>
          brokerMetadataMap += (logDir -> brokerMetadata)
          brokerMetadataSet += brokerMetadata
        }
      } catch {
        case e: IOException =>
          offlineDirs += logDir
          error(s"Fail to read $brokerMetaPropsFile under log directory $logDir", e)
      }
    }

    if (brokerMetadataSet.size > 1) {
      val builder = new StringBuilder

      for ((logDir, brokerMetadata) <- brokerMetadataMap)
        builder ++= s"- $logDir -> $brokerMetadata\n"

      throw new InconsistentBrokerMetadataException(
        s"BrokerMetadata is not consistent across log.dirs. This could happen if multiple brokers shared a log directory (log.dirs) " +
        s"or partial data was manually copied from another broker. Found:\n${builder.toString()}"
      )
    } else if (brokerMetadataSet.size == 1)
      (brokerMetadataSet.last, offlineDirs)
    else
      (BrokerMetadata(-1, None), offlineDirs)
  }


  /**
   * Checkpoint the BrokerMetadata to all the online log.dirs
   *
   * @param brokerMetadata
   */
  private def checkpointBrokerMetadata(brokerMetadata: BrokerMetadata) = {
    for (logDir <- config.logDirs if logManager.isLogDirOnline(new File(logDir).getAbsolutePath)) {
      val checkpoint = brokerMetadataCheckpoints(logDir)
      checkpoint.write(brokerMetadata)
    }
  }

  /**
   * Generates new brokerId if enabled or reads from meta.properties based on following conditions
   * <ol>
   * <li> config has no broker.id provided and broker id generation is enabled, generates a broker.id based on Zookeeper's sequence
   * <li> config has broker.id and meta.properties contains broker.id if they don't match throws InconsistentBrokerIdException
   * <li> config has broker.id and there is no meta.properties file, creates new meta.properties and stores broker.id
   * <ol>
   *
   * @return The brokerId.
   */
  private def getOrGenerateBrokerId(brokerMetadata: BrokerMetadata): Int = {
    val brokerId = config.brokerId

    if (brokerId >= 0 && brokerMetadata.brokerId >= 0 && brokerMetadata.brokerId != brokerId)
      throw new InconsistentBrokerIdException(
        s"Configured broker.id $brokerId doesn't match stored broker.id ${brokerMetadata.brokerId} in meta.properties. " +
        s"If you moved your data, make sure your configured broker.id matches. " +
        s"If you intend to create a new broker, you should remove all data in your data directories (log.dirs).")
    else if (brokerMetadata.brokerId < 0 && brokerId < 0 && config.brokerIdGenerationEnable) // generate a new brokerId from Zookeeper
      generateBrokerId
    else if (brokerMetadata.brokerId >= 0) // pick broker.id from meta.properties
      brokerMetadata.brokerId
    else
      brokerId
  }

  /**
    * Return a sequence id generated by updating the broker sequence id path in ZK.
    * Users can provide brokerId in the config. To avoid conflicts between ZK generated
    * sequence id and configured brokerId, we increment the generated sequence id by KafkaConfig.MaxReservedBrokerId.
    */
  private def generateBrokerId: Int = {
    try {
      zkClient.generateBrokerSequenceId() + config.maxReservedBrokerId
    } catch {
      case e: Exception =>
        error("Failed to generate broker.id due to ", e)
        throw new GenerateBrokerIdException("Failed to generate broker.id", e)
    }
  }
}
