/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.connect.channel;

import static java.util.stream.Collectors.toList;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.cmdb.CmdbManager;
import org.apache.iceberg.connect.cmdb.CmdbManagerFactory;
import org.apache.iceberg.connect.cmdb.dto.ddl.request.DdlExecutionRequest;
import org.apache.iceberg.connect.cmdb.model.DdlStatus;
import org.apache.iceberg.connect.common.Alert;
import org.apache.iceberg.connect.common.AlertTarget;
import org.apache.iceberg.connect.data.Offset;
import org.apache.iceberg.connect.data.RecordUtils;
import org.apache.iceberg.connect.data.SinkWriter;
import org.apache.iceberg.connect.data.SinkWriterResult;
import org.apache.iceberg.connect.events.DDLComplete;
import org.apache.iceberg.connect.events.DDLReady;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.LastDMLInfo;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.connect.metrics.PendingRecordCustomMetrics;
import org.apache.iceberg.connect.metrics.ProcessCustomMetrics;
import org.apache.iceberg.relocated.com.google.common.base.Stopwatch;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

class Worker extends Channel {

  private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
  private final IcebergSinkConfig config;
  private final SinkTaskContext context;
  private final SinkWriter sinkWriter;
  private final CmdbManager cmdbManager;
  private long sinkDDLVersion;
  private final Alert alert;
  private final Set<TopicPartition> pausedPartitions;
  private final ProcessCustomMetrics processCustomMetrics;
  private final PendingRecordCustomMetrics pendingRecordCustomMetrics;
  private final String clusterName;
  private final String tableName;
  private final TableIdentifier tableIdentifier;
  private final Map<Integer, SinkRecord> lastProcessedRecords;

  Worker(
      IcebergSinkConfig config,
      KafkaClientFactory clientFactory,
      SinkWriter sinkWriter,
      SinkTaskContext context,
      ProcessCustomMetrics processCustomMetrics,
      PendingRecordCustomMetrics pendingRecordCustomMetrics) {
    // pass transient consumer group ID to which we never commit offsets
    super(
        "worker",
        config.controlGroupIdPrefix() + UUID.randomUUID(),
        config,
        clientFactory,
        context);

    this.config = config;
    this.context = context;
    this.sinkWriter = sinkWriter;
    this.cmdbManager = CmdbManagerFactory.getInstance(config);
    this.alert = new Alert(config);
    this.pausedPartitions = Sets.newHashSet();
    this.processCustomMetrics = processCustomMetrics;
    this.clusterName = config.hadoopClusterName();
    this.lastProcessedRecords = Maps.newHashMap();
    this.pendingRecordCustomMetrics = pendingRecordCustomMetrics;

    // For dynamic table routing, config.tables() is null
    if (config.tables() != null && !config.tables().isEmpty()) {
      this.tableName = config.tables().get(0);
      this.tableIdentifier = TableIdentifier.parse(config.tables().get(0));
    } else {
      this.tableName = null;
      this.tableIdentifier = null;
    }
  }

  void process() {
    consumeAvailable(Duration.ZERO);
  }

  @Override
  protected boolean receive(Envelope envelope) {
    Event event = envelope.event();
    if (envelope.event().type() == PayloadType.DDL_COMPLETE) {
      long ddlVersionInEvent = ((DDLComplete) envelope.event().payload()).ddlVersion();
      // 다른 워커노드에서 DDL 처리가 이뤄진 경우를 고려해, 직접 API 호출하여 최신 정보를 받고 CmdbResponseCache 갱신
      cmdbManager.fetchDdlExecutions(null, DdlStatus.PROCESSED);
      sinkDDLVersion = cmdbManager.getLatestDdlVersion();

      Level level = ddlVersionInEvent == sinkDDLVersion ? Level.INFO : Level.WARN;
      String msg =
          String.format(
              Locale.ROOT,
              "[DDL connector] DDL complete event received. current version in - (event) %d, (cmdb) %d",
              ddlVersionInEvent,
              sinkDDLVersion);

      alert.sendNotificationMessage(msg, AlertTarget.MANAGER, level);

      pausedPartitions.forEach(
          partition ->
              LOG.info(
                  "[DDL connector] Resuming paused partition: {}-{}",
                  partition.topic(),
                  partition.partition()));
      pausedPartitions.forEach(context::resume);
      pausedPartitions.clear();

      return true;
    }
    if (event.payload().type() != PayloadType.START_COMMIT) {
      return false;
    }
    LOG.info("[DDL connector] Worker received start commit event.");
    LOG.info(
        "[DDL connector] Control topic envelope partition: {}, offset: {}",
        envelope.partition(),
        envelope.offset());

    SinkWriterResult results = sinkWriter.completeWrite();

    // include all assigned topic partitions even if no messages were read
    // from a partition, as the coordinator will use that to determine
    // when all data for a commit has been received
    List<TopicPartitionOffset> assignments =
        context.assignment().stream()
            .map(
                tp -> {
                  Offset offset = results.sourceOffsets().get(tp);
                  if (offset == null) {
                    offset = Offset.NULL_OFFSET;
                  }
                  return new TopicPartitionOffset(
                      tp.topic(), tp.partition(), offset.offset(), offset.timestamp());
                })
            .collect(Collectors.toList());

    UUID commitId = ((StartCommit) event.payload()).commitId();

    List<Event> events =
        results.writerResults().stream()
            .map(
                writeResult ->
                    new Event(
                        config.connectGroupId(),
                        new DataWritten(
                            writeResult.partitionStruct(),
                            commitId,
                            TableReference.of(config.catalogName(), writeResult.tableIdentifier()),
                            writeResult.dataFiles(),
                            writeResult.deleteFiles())))
            .collect(Collectors.toList());

    Event readyEvent = new Event(config.connectGroupId(), new DataComplete(commitId, assignments));
    events.add(readyEvent);

    send(events, results.sourceOffsets());

    return true;
  }

  @Override
  void stop() {
    super.stop();
    sinkWriter.close();
  }

  void save(Collection<SinkRecord> sinkRecords) {
    // Kakao CDC Enabled가 아닌 경우
    if (!config.kakaoCdcEnabled()) {
      LOG.info(
          "[DDL connector] Kakao CDC is disabled. Process {} records as DML only.",
          sinkRecords == null ? 0 : sinkRecords.size());
      sinkWriter.save(sinkRecords);

      return;
    }

    // control topic의 메세지를 받고 DDLComplete 이벤트가 발생한 경우 context resume
    consumeAvailable(Duration.ZERO);
    // Control topic 내 메세지를 처리 하고나서는 Control topic offset 커밋 처리
    commitConsumerOffsets();

    if (sinkRecords == null || sinkRecords.isEmpty()) {
      return;
    }

    sinkDDLVersion = cmdbManager.getLatestDdlVersion();
    LOG.info(
        "[DDL connector] Worker write start. current ddl_version: {}, sinkRecords count: {}",
        sinkDDLVersion,
        sinkRecords.size());
    /*
     Iceberg Sink Connector는 DML 메세지와 DDL 메세지를 구분하여 처리한다.
     0번 partition으로 DDL이 인입되는 특성상 processDDL 구문은 0번 partition을 consuming 하는 task에서만
     실행되게 되어있다.

     0번 partition을 consume하는 task가 아닌 task들은 processDML 구문을 실행하게 되는데 이때 현재 sinkDDLVersion과
     상이한 레코드가 들어온다면 해당 메세지의 offset으로 context를 설정하고 해당 파티션을 pause한다.
     예를들면 아래와 같은 형태로 메세지가 올 경우 offset은 10으로 설정 된다.
     offset      | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | 13
     ddl_version | 0 | 0 | 0 | 0 | 0 | 1  | 1  | 1  | 1

     0번 partition을 consume하는 task에서 아래와 같은 형태로 메세지가 올 경우 offset은 DDL이 발생한 메세지의 offset인
     9로 설정된다. 이후 context는 pause되고 9번 오프셋에 대한 DDL 구문을 DDLReady 이벤트로 만들어 Coordinator로 전송하게 된다.
     이후 DDL을 성공적으로 적용하고 Coordinator로부터 DDLComplete 이벤트를 받으면 context를 resume하게 된다.
     offset      | 5 | 6 | 7 | 8 | 9     | 10 | 11 | 12 | 13
     ddl_version | 0 | 0 | 0 | 0 | DDL A | 1  | 1  | 1  | 1

     아래 처럼 sinkRecords 내에 DDL A와 DDL B가 모두 존재하는 경우에 earliestDdlRecord 를 찾아 offset을 설정하게 된다.
     이때 offset은 DDL A의 offset인 8로 설정된다. 이후 파티션이 pause되고 DDL A의 ddl 구문을 처리한 후 DDL B를 처리하게 된다.
     DDL
     offset      | 5 | 6 | 7 | 8     | 9 | 10 | 11 | 12    | 13
     ddl_version | 0 | 0 | 0 | DDL A | 1 | 1  | 1  | DDL B | 2

     아래 기술할 경우는 유실이 발생할 수 있는 경우의 수를 설명하기 위한 예시이다.
     (하지만 source connector 단에서 아래와 같은 형태로 메세지 인입은 이루어지지 않는다고 한다.)
     아래와 같은 경우처럼 DDL B가 DDL A보다 먼저 들어온 경우에는 DDL B가 반영되고 이후 DDL A는 ddl_version이 낮아 반영되지 않는다.
     따라서 10, 11번 offset을 가진 메세지에 대한 DML 처리가 이루어지지 않게 된다.
     offset      | 5 | 6 | 7 | 8     | 9     | 10 | 11 | 12 | 13
     ddl_version | 0 | 0 | 0 | DDL B | DDL A | 1  | 1  | 2  | 2

     아래와 같은 경우로 메세지가 들어올 경우에는 DDL이 발생한 메세지의 offset인 9로 설정되어 offset 8번에 대한 메세지는 유실될 수 있다.
     offset      | 5 | 6 | 7 | 8 | 9     | 10 | 11 | 12 | 13
     ddl_version | 0 | 0 | 0 | 1 | DDL A | 1  | 1  | 2  | 2
    */
    Collection<SinkRecord> mergedRecords = Lists.newArrayList(sinkRecords);
    // DDL 메세지와 DML 메세지 분리
    Stopwatch ddlDmlSplitStopwatch = Stopwatch.createStarted();
    Collection<SinkRecord> ddlRecords =
        mergedRecords.stream().filter(RecordUtils::isDDLMessage).collect(toList());
    ddlDmlSplitStopwatch.stop();
    LOG.trace(
        "[DDL connector] [PERF] DDL DML split took {} ms",
        ddlDmlSplitStopwatch.elapsed().toMillis());

    LOG.info("[DDL connector] extract DDL message from mergedRecords: {}", ddlRecords);
    LOG.info("[DDL connector] before remove ddls, mergedRecords count: {}", mergedRecords.size());
    mergedRecords.removeAll(ddlRecords);
    LOG.info("[DDL connector] after remove ddls, mergedRecords count: {}", mergedRecords.size());

    Stopwatch processDMLStopwatch = Stopwatch.createStarted();
    processDML(mergedRecords);
    processDMLStopwatch.stop();
    LOG.trace(
        "[DDL connector] [PERF] processDML took {} ms", processDMLStopwatch.elapsed().toMillis());

    // DML 처리 메트릭 업데이트
    if (!mergedRecords.isEmpty() && this.processCustomMetrics != null) {
      this.processCustomMetrics.recordDmlProcessLatency(mergedRecords, System.currentTimeMillis());
    }

    Stopwatch processDDLStopwatch = Stopwatch.createStarted();
    processDDL(ddlRecords);
    processDDLStopwatch.stop();
    LOG.trace(
        "[DDL connector] [PERF] processDDL took {} ms", processDDLStopwatch.elapsed().toMillis());
  }

  public void processDML(Collection<SinkRecord> dmls) {
    // 각 파티션별로 DDL version이 다른 레코드가 들어왔을때 가장 earliest한 offset을 찾기 위한 변수
    Map<Integer, Long> earliestOffsets = Maps.newHashMap();

    // DML 메세지 처리
    for (SinkRecord record : dmls) {
      long incomingDdlVersion = RecordUtils.ddlVersionFromHeader(record);

      // 이전 DDL 버전에 해당하는 DML 메세지가 들어온 경우 Skip.
      // DDL이 발생하고 context가 pause된 상태에서 다시 resume 되었을때
      // context의 offset이 새로운 ddl_version을 가진 레코드 중 가장 빠른 레코드의 offset으로 변경되기 때문에
      // 이전 DDL 버전에 해당하는 DML 메세지가 다시 들어올 수 있으나 아래 로직에서 필터링됨.
      if (incomingDdlVersion < sinkDDLVersion) {
        LOG.info(
            "[DDL connector] Incoming DDL version is lower than the current DDL version. Skip DML Incoming: {}, Current: {}",
            incomingDdlVersion,
            sinkDDLVersion);
        increaseSourceOffset(record);
        continue;
      }

      // sinkDDLVersion이 없는 경우 incomingDdlVersion으로 업데이트
      if (sinkDDLVersion == CmdbManager.UNDEFINED_DDL_VERSION) {
        LOG.info(
            "[DDL connector] DDL version not found. Update DDL version to incoming DDL version: {}",
            incomingDdlVersion);
        cmdbManager.postDdlExecution(
            new DdlExecutionRequest(clusterName, tableName, incomingDdlVersion));
        sinkDDLVersion = incomingDdlVersion;
        sinkWriter.save(record);
        lastProcessedRecords.put(record.kafkaPartition(), record);

        // pending records 가 없는 경우 metric 을 0 으로 초기화한다.
        this.pendingRecordCustomMetrics.updatePendingRecordOffsets(record.kafkaPartition(), 0L);
        continue;
      }
      // 테이블과 동일한 버전의 ddl_version을 가진 DML 메세지는 반영
      if (incomingDdlVersion == sinkDDLVersion) {
        sinkWriter.save(record);
        lastProcessedRecords.put(record.kafkaPartition(), record);

        // pending records 가 없는 경우 metric 을 0 으로 초기화한다.
        this.pendingRecordCustomMetrics.updatePendingRecordOffsets(record.kafkaPartition(), 0L);

      } else {
        // sinkRecord의 DDL 버전이 테이블의 DDL 버전보다 높은 경우, 나중에 반영해야 하는 레코드.
        // 이 경우 해당 파티션을 pause하고 dmls 중 sinkDDLVersion 보단 높지만 그 중에서 가장 낮은 offset을 찾아서
        // context를 변경해준다.
        LOG.info(
            "[DDL connector] Incoming DDL version is higher than the current DDL version. Incoming: {}, Current: {}",
            incomingDdlVersion,
            sinkDDLVersion);

        // 해당 파티션의 가장 빠른 offset을 찾기 위해 비교
        if (earliestOffsets.containsKey(record.kafkaPartition())) {
          if (earliestOffsets.get(record.kafkaPartition()) > record.kafkaOffset()) {
            LOG.info(
                "[DDL connector] update partition offset to the record offset. earliestOffsets: {}",
                earliestOffsets);
            earliestOffsets.put(record.kafkaPartition(), record.kafkaOffset());
          }
        } else {
          earliestOffsets.put(record.kafkaPartition(), record.kafkaOffset());
        }
      }
    }

    for (Map.Entry<Integer, Long> entry : earliestOffsets.entrySet()) {
      changeOffsetAndPause(entry.getKey(), entry.getValue());

      // pending records 가 존재하는 경우 가장 빠른 offset 을 보여준다.
      this.pendingRecordCustomMetrics.updatePendingRecordOffsets(entry.getKey(), entry.getValue());
    }
  }

  public void processDDL(Collection<SinkRecord> ddls) {
    // sinkRecords 중 가장 ddl_version이 낮은 레코드를 찾음
    SinkRecord earliestDdlRecord = null;
    // 하나의 sinkRecords 묶음에 현재 테이블 ddl version보다 높은 메세지가 여럿 있을 경우, earliestDdlRecord를 구한다.
    for (SinkRecord record : ddls) {
      long incomingDdlVersion = RecordUtils.ddlVersionFromHeader(record);

      // 현재 DDL 버전 이하면 skip
      if (incomingDdlVersion <= sinkDDLVersion) {
        LOG.info(
            "[DDL connector] Incoming DDL version is lower than or equal to the current DDL version. Skip DDL Incoming: {}, Current: {}",
            incomingDdlVersion,
            sinkDDLVersion);
      } else {
        if (earliestDdlRecord == null || earliestDdlRecord.kafkaOffset() > record.kafkaOffset()) {
          earliestDdlRecord = record;
        }
      }
    }
    // earliestDdlRecord가 null이 아닌경우 ddl을 반영시키기 위한 Event 생성
    if (earliestDdlRecord != null) {
      // 현재 인입된 ddl 메세지들 중 가장 ddl_version이 낮은 레코드의 ddl 구문을 처리하고 난 이후 메세지에서는
      // 그보다 더 낮은 ddl_version을 가진 레코드가 들어올 수 없다고 상정함.
      // (그렇지 않으면 더 낮은 ddl_version을 가진 레코드가 언제 들어오는지 모르기 때문에 계속 기다려야 함)
      // 위와 같이 상정했기 때문에 context를 pause 하고 offset을 해당 레코드의 offset으로 변경함.
      // Partition 0을 consume하고 있는 task-0은 sinkRecords내 ddls들을 순차적으로 처리함을 의미함.
      LOG.info("[DDL connector] earliestDdlRecord: {}", earliestDdlRecord);
      LOG.info(
          "[DDL connector] earliestDdlRecord exist! pause partition. Partition: {}, Offset: {}",
          earliestDdlRecord.kafkaPartition(),
          earliestDdlRecord.kafkaOffset());
      // DDL 메세지의 파티션을 pause,
      TopicPartition topicPartition =
          new TopicPartition(earliestDdlRecord.topic(), earliestDdlRecord.kafkaPartition());
      context.pause(topicPartition);
      LOG.info("[DDL connector] add partition to pausedPartitions. Partition: {}", topicPartition);
      pausedPartitions.add(topicPartition);
      LOG.info(
          "[DDL connector] set context offset to earliest DDL record. "
              + "Partition: {}, Offset: {}",
          earliestDdlRecord.kafkaPartition(),
          earliestDdlRecord.kafkaOffset() + 1);

      context.offset(topicPartition, earliestDdlRecord.kafkaOffset() + 1);

      Struct ddlStruct = (Struct) earliestDdlRecord.value();

      String ddl = ddlStruct.getString("ddl");
      long ddlVersion =
          Long.parseLong(
              RecordUtils.headerValueToString(
                  earliestDdlRecord.headers().lastWithName("ddl_version").value()));
      LOG.info("Worker received DDL message. DDL: {}, DDL Version: {}", ddl, ddlVersion);
      String msg =
          String.format(
              Locale.ROOT,
              "Worker received DDL message. DDL: %s, DDL Version: %d",
              ddl,
              ddlVersion);

      // CMDB 에 DDL 수신 이력을 기록한다.
      registerDdlExecution(earliestDdlRecord);

      // Coordinator로 DDL 메세지 전송
      alert.sendNotificationMessage(msg, AlertTarget.MANAGER, Level.INFO);
      sendDDLReady(earliestDdlRecord, ddlVersion);

      // DDL 처리 메트릭 업데이트
      if (this.processCustomMetrics != null) {
        this.processCustomMetrics.recordDdlProcessLatency(
            earliestDdlRecord, System.currentTimeMillis());
      }
    }
  }

  private void increaseSourceOffset(SinkRecord record) {
    OffsetDateTime timestamp =
        record.timestamp() == null
            ? null
            : OffsetDateTime.ofInstant(Instant.ofEpochMilli(record.timestamp()), ZoneOffset.UTC);
    sinkWriter.putSourceOffset(
        new TopicPartition(record.topic(), record.kafkaPartition()),
        new Offset(record.kafkaOffset() + 1, timestamp));
  }

  public void registerDdlExecution(SinkRecord record) {
    long incomingDdlVersion = RecordUtils.ddlVersionFromHeader(record);
    String ddl = ((Struct) record.value()).getString("ddl");

    cmdbManager.checkDdlExecutionOrCreate(incomingDdlVersion, ddl);
  }

  public void changeOffsetAndPause(Integer partition, Long offset) {
    LOG.info(
        "[DDL connector] change partition offset and pause.  Partition: {}, Offset: {}",
        partition,
        offset);

    TopicPartition topicPartition = new TopicPartition(config.topics(), partition);
    context.pause(topicPartition);
    LOG.info(
        "[DDL connector] While processing DML records, add partition to pausedPartitions. Partition: {}",
        topicPartition);
    pausedPartitions.add(topicPartition);
    context.offset(topicPartition, offset);
  }

  private void sendDDLReady(SinkRecord record, long incomingDdlVersion) {
    String ddl = ((Struct) record.value()).getString("ddl");

    List<LastDMLInfo> lastDMLInfoList = Lists.newArrayList();

    Header lastDMLInfo = record.headers().lastWithName("last_dml_info");
    if (lastDMLInfo != null) {
      Map<?, ?> lastDmlInfoValue = (Map<?, ?>) lastDMLInfo.value();
      lastDmlInfoValue.forEach(
          (key, value) -> {
            Integer partition = Integer.parseInt(key.toString());
            String gtId = ((Map<?, ?>) value).get("gtid").toString();
            Long pos = Long.parseLong(((Map<?, ?>) value).get("pos").toString());
            Integer row = Integer.parseInt(((Map<?, ?>) value).get("row").toString());

            lastDMLInfoList.add(new LastDMLInfo(partition, gtId, pos, row));
          });
    }

    lastDMLInfoList.forEach(
        info ->
            LOG.info(
                "[DDL connector] Last DML info of previous ddl version . Partition: {}, GTID: {}",
                info.getPartition(),
                info.getGtId()));

    Event ddlReady =
        new Event(
            config.connectGroupId(),
            new DDLReady(
                TableReference.of(config.catalogName(), tableIdentifier),
                ddl,
                incomingDdlVersion,
                lastDMLInfoList));
    send(ddlReady);
    LOG.info(
        "[DDL connector] DDLReady event sent. DDL: {}, DDL Version: {}", ddl, incomingDdlVersion);
  }
}
