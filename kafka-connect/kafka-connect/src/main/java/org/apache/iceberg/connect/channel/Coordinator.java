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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotAncestryValidator;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.auth.KerberosAuthManager;
import org.apache.iceberg.connect.cmdb.CmdbManager;
import org.apache.iceberg.connect.cmdb.CmdbManagerFactory;
import org.apache.iceberg.connect.cmdb.dto.ddl.request.DdlExecutionPatchRequest;
import org.apache.iceberg.connect.cmdb.dto.ddl.response.DdlExecutionResponse;
import org.apache.iceberg.connect.cmdb.dto.dml.request.DmlStatusPatchRequest;
import org.apache.iceberg.connect.cmdb.dto.dml.response.DmlStatusResponse;
import org.apache.iceberg.connect.cmdb.model.DdlStatus;
import org.apache.iceberg.connect.cmdb.model.PartitionStatus;
import org.apache.iceberg.connect.common.Alert;
import org.apache.iceberg.connect.common.AlertTarget;
import org.apache.iceberg.connect.data.RecordUtils;
import org.apache.iceberg.connect.data.SchemaUtils;
import org.apache.iceberg.connect.events.CommitComplete;
import org.apache.iceberg.connect.events.CommitToTable;
import org.apache.iceberg.connect.events.DDLComplete;
import org.apache.iceberg.connect.events.DDLReady;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.LastDMLInfo;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.http.HttpClientServiceFactory;
import org.apache.iceberg.connect.translator.ApiDDLTranslator;
import org.apache.iceberg.connect.translator.DDL;
import org.apache.iceberg.connect.translator.DDLTranslator;
import org.apache.iceberg.connect.translator.DDLType;
import org.apache.iceberg.connect.translator.TranslationResponse;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

class Coordinator extends Channel {

  private static final Logger LOG = LoggerFactory.getLogger(Coordinator.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String COMMIT_ID_SNAPSHOT_PROP = "kafka.connect.commit-id";
  private static final String TASK_ID_SNAPSHOT_PROP = "kafka.connect.task-id";
  private static final String VALID_THROUGH_TS_SNAPSHOT_PROP = "kafka.connect.valid-through-ts";
  private static final Duration POLL_DURATION = Duration.ofSeconds(1);

  private final Alert alert;
  private final Catalog catalog;
  private final IcebergSinkConfig config;
  private final int totalPartitionCount;
  private final String snapshotOffsetsProp;
  private final ExecutorService exec;
  private final CommitState commitState;
  private volatile boolean terminated;

  private final DDLTranslator translator;
  private final CmdbManager cmdbManager;
  private final boolean convertTimezoneRequired;

  private KerberosAuthManager authManager;

  Coordinator(
      Catalog catalog,
      IcebergSinkConfig config,
      Collection<MemberDescription> members,
      KafkaClientFactory clientFactory,
      SinkTaskContext context) {
    this(
        catalog,
        config,
        members,
        clientFactory,
        context,
        new ApiDDLTranslator(HttpClientServiceFactory.getInstance(config.connectAdminUrl())),
        CmdbManagerFactory.getInstance(config));
  }

  // 테스트용 생성자: translator와 cmdbManager를 주입받음
  Coordinator(
      Catalog catalog,
      IcebergSinkConfig config,
      Collection<MemberDescription> members,
      KafkaClientFactory clientFactory,
      SinkTaskContext context,
      DDLTranslator translator,
      CmdbManager cmdbManager) {
    // pass consumer group ID to which we commit low watermark offsets
    super("coordinator", config.connectGroupId(), config, clientFactory, context);

    this.alert = new Alert(config);
    this.catalog = catalog;
    this.config = config;
    this.translator = translator;
    this.cmdbManager = cmdbManager;
    if (config.kerberosAuthentication()) {
      this.authManager = KerberosAuthManager.getInstance(config);
    }
    this.totalPartitionCount =
        members.stream().mapToInt(desc -> desc.assignment().topicPartitions().size()).sum();
    this.snapshotOffsetsProp =
        String.format(
            "kafka.connect.offsets.%s.%s", config.controlTopic(), config.connectGroupId());
    this.exec =
        new ThreadPoolExecutor(
            config.commitThreads(),
            config.commitThreads(),
            config.keepAliveTimeoutInMs(),
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("iceberg-committer" + "-%d")
                .build());
    this.commitState = new CommitState(config);
    this.convertTimezoneRequired =
        config.transforms() != null
            && config.transforms().contains(IcebergSinkConfig.TIMEZONE_SMT_NAME);

    if (config.kerberosAuthentication()) {
      this.authManager = KerberosAuthManager.getInstance(config);
    }
  }

  void process() {
    // KerberosAuthenticationManager를 사용하여 인증된 컨텍스트에서 실행
    try {
      Runnable action =
          () -> {
            if (commitState.isCommitIntervalReached()) {
              // send out begin commit
              commitState.startNewCommit();
              Event event =
                  new Event(
                      config.connectGroupId(), new StartCommit(commitState.currentCommitId()));
              send(event);
              LOG.info("Commit {} initiated", commitState.currentCommitId());
            }

            consumeAvailable(POLL_DURATION);

            if (commitState.isCommitTimedOut()) {
              commit(true);
            }
          };

      if (authManager != null) {
        authManager.executeWithAuth(
            () -> {
              action.run();
              return null;
            });
      } else {
        action.run();
      }
    } catch (Exception e) {
      LOG.error("Coordinator - failed during process", e);
      throw new RuntimeException("Failed in Coordinator", e);
    }
  }

  @Override
  protected boolean receive(Envelope envelope) {
    if (config.kakaoCdcEnabled()) {
      return receiveWithCdc(envelope);
    } else {
      return receiveSimple(envelope);
    }
  }

  /** Simple receive logic without DDL handling. This is the original main branch behavior. */
  private boolean receiveSimple(Envelope envelope) {
    switch (envelope.event().payload().type()) {
      case DATA_WRITTEN:
        commitState.addResponse(envelope);
        return true;
      case DATA_COMPLETE:
        commitState.addReady(envelope);
        if (commitState.isCommitReady(totalPartitionCount)) {
          commit(false);
        }
        return true;
    }
    return false;
  }

  /** Receive logic with DDL handling for CDC mode. */
  private boolean receiveWithCdc(Envelope envelope) {
    LOG.info("[DDL connector] Coordinator envelope event type: {}", envelope.event().type());
    switch (envelope.event().payload().type()) {
      case DATA_WRITTEN:
        commitState.addResponse(envelope);
        return true;
      case DATA_COMPLETE:
        commitState.addReady(envelope);
        if (commitState.isCommitReady(totalPartitionCount)) {
          commit(false);
        }
        return true;
      case DDL_READY:
        DDLReady ddlReady = (DDLReady) envelope.event().payload();
        LOG.info(
            "[DDL connector] Coordinator Received DDL ready event. ddl: {}, ddl_version: {}",
            ddlReady.ddl(),
            ddlReady.ddlVersion());
        commitState.addDDL(envelope);
        if (!commitState.isCommitInProgress()) {
          // DDL 적용 전에 Worker들의 버퍼 데이터를 먼저 커밋해야 함
          // StartCommit 이벤트를 보내서 Worker들이 DATA_WRITTEN/DATA_COMPLETE를 보내도록 함
          commitState.startNewCommit();
          Event event =
              new Event(config.connectGroupId(), new StartCommit(commitState.currentCommitId()));
          send(event);
          LOG.info(
              "[DDL connector] Started new commit with commit-id={} for applying ddl. "
                  + "Sent StartCommit to workers to flush pending data.",
              commitState.currentCommitId().toString());
          // commit(true)를 바로 호출하지 않음 - Worker 응답을 기다림 (DATA_WRITTEN이 전부 완료되면 커밋이 이루어지도록)
          // 응답이 오면 DATA_COMPLETE 핸들러에서 commit이 실행되거나 timeout이 되면 process()에서 commit(true)가 실행됨
          // 곧바로 partial commit을 실행하지 않아도 아래와 같은 이유로 문제가 없다.
          //  1. 새 DDL 버전의 DML은 incomingDdlVersion > sinkDDLVersion 체크로 save()가 호출되지 않음
          //  2. 해당 파티션은 pause되고 offset이 되돌려짐
          //  3. DDL이 적용되고 sinkDDLVersion이 업데이트된 후에야 해당 레코드들이 재처리됨
        }
    }
    return false;
  }

  private void commit(boolean partialCommit) {
    try {
      LOG.info(
          "Processing commit after responses for {}, isPartialCommit {}",
          commitState.currentCommitId(),
          partialCommit);
      doCommit(partialCommit);
    } catch (Exception e) {
      LOG.warn("Commit failed, will try again next cycle", e);
    } finally {
      commitState.endCurrentCommit();
    }
  }

  private void doCommit(boolean partialCommit) {
    if (config.kakaoCdcEnabled()) {
      doCommitWithCdc(partialCommit);
    } else {
      doCommitSimple(partialCommit);
    }
  }

  /** Simple commit logic without CDC/DDL handling. This is the original main branch behavior. */
  private void doCommitSimple(boolean partialCommit) {
    Map<TableReference, List<Envelope>> commitMap = commitState.tableCommitMap();
    OffsetDateTime validThroughTs = commitState.validThroughTs(partialCommit);

    Tasks.foreach(commitMap.entrySet())
        .executeWith(exec)
        .stopOnFailure()
        .run(
            entry -> {
              commitToTable(
                  entry.getKey(), entry.getValue(), controlTopicOffsets(), validThroughTs);
            });

    // we should only get here if all tables committed successfully...
    commitConsumerOffsets();
    commitState.clearResponses();

    Event event =
        new Event(
            config.connectGroupId(),
            new CommitComplete(commitState.currentCommitId(), validThroughTs));
    send(event);

    LOG.info(
        "Commit {} complete, committed to {} table(s), valid-through {}",
        commitState.currentCommitId(),
        commitMap.size(),
        validThroughTs);
  }

  /** Commit logic with CDC/DDL handling, CMDB integration, and alerts. */
  private void doCommitWithCdc(boolean partialCommit) {
    Map<TableReference, List<Envelope>> commitMap = commitState.tableCommitMap();
    OffsetDateTime validThroughTs = commitState.validThroughTs(partialCommit);
    boolean hasDDL = !commitState.ddlBuffer().isEmpty();

    LOG.info(
        "[DDL connector] doCommit called - partialCommit: {}, commitMap size: {}, "
            + "ddlBuffer size: {}",
        partialCommit,
        commitMap.size(),
        commitState.ddlBuffer().size());

    if (!commitMap.isEmpty()) {
      commitMap.forEach(
          (tableId, envelopes) ->
              LOG.info(
                  "[DDL connector] commitMap entry - table: {}, envelopes: {}",
                  tableId,
                  envelopes.size()));
    } else {
      LOG.info("[DDL connector] doCommit commitMap is EMPTY - no DML data to commit");
      if (hasDDL) {
        LOG.warn(
            "[DDL connector] DDL exists but commitMap is empty! "
                + "DDL will be applied without prior DML commit.");
      }
    }

    Tasks.foreach(commitMap.entrySet())
        .executeWith(exec)
        .stopOnFailure()
        .run(
            entry -> {
              Runnable commitAction =
                  () ->
                      commitToTable(
                          entry.getKey(), entry.getValue(), controlTopicOffsets(), validThroughTs);
              if (authManager != null) {
                authManager.executeWithAuth(
                    () -> {
                      commitAction.run();
                      return null;
                    });
              } else {
                commitAction.run();
              }
            });

    List<DDLReady> pendingDDLs = Lists.newArrayList();
    boolean ddlExists = !commitState.ddlBuffer().isEmpty();
    List<DDLReady> processedDDLs = Lists.newArrayList();
    while (!commitState.ddlBuffer().isEmpty()) {
      DDLReady ddlReady = commitState.popDDL();
      try {
        if (!applyDDLToTable(ddlReady, TableIdentifier.parse(config.tables().get(0)))) {
          long currentVerion = cmdbManager.getLatestDdlVersion();
          if (ddlReady.ddlVersion() > currentVerion) {
            alert.sendNotificationMessage(
                "Pending ddl occurred! incoming: " + ddlReady.ddl() + " current: " + currentVerion,
                AlertTarget.MANAGER,
                Level.WARN);
            pendingDDLs.add(ddlReady);
          } else {
            LOG.info(
                "[DDL connector] DDL version {} is already applied, skipping",
                ddlReady.ddlVersion());
          }
        } else {
          processedDDLs.add(ddlReady);
        }
      } catch (RuntimeException e) {
        LOG.error(
            "[DDL connector] Error applying ddl: {}, ddl_version: {}. restore DDLEvent to ddlBuffer",
            ddlReady.ddl(),
            ddlReady.ddlVersion(),
            e);
        pendingDDLs.add(ddlReady);
        break;
      }
    }

    if (ddlExists) {
      if (pendingDDLs.isEmpty()) {
        long latestDdlVersion = cmdbManager.getLatestDdlVersion();
        LOG.info(
            "[DDL connector] DDL Successfully applied. send DDLComplete event for ddl_version: {}",
            latestDdlVersion);
        Event event = new Event(config.connectGroupId(), new DDLComplete(latestDdlVersion));
        send(event);
      } else {
        LOG.info(
            "[DDL connector] Pending DDLs: {}",
            pendingDDLs.stream().map(DDLReady::ddlVersion).collect(Collectors.toList()));
      }
    }
    commitState.replaceDDLBuffer(pendingDDLs);
    // we should only get here if all tables committed successfully...
    commitConsumerOffsets();
    commitState.clearResponses();

    Event event =
        new Event(
            config.connectGroupId(),
            new CommitComplete(commitState.currentCommitId(), validThroughTs));
    send(event);

    LOG.info(
        "Commit {} complete, committed to {} table(s), valid-through {}",
        commitState.currentCommitId(),
        commitMap.size(),
        validThroughTs);

    for (DDLReady doneDDL : processedDDLs) {
      alert.sendNotificationMessage(
          String.format(
              """
                  DDL이 성공적으로 처리되었습니다
          DDL: %s
          DDL version: %s
          """,
              doneDDL.ddl(), doneDDL.ddlVersion()),
          AlertTarget.ALL,
          Level.INFO);
    }
  }

  private boolean applyDDLToTable(DDLReady ddlReady, TableIdentifier tableIdentifier) {
    // DDL 이벤트에 기록되어 있는, 이전 버전의 마지막 DML 정보 획득
    List<LastDMLInfo> lastDMLInfoList = ddlReady.lastDMLInfoList();

    long currentDdlVersion = cmdbManager.getLatestDdlVersion();
    LOG.info(
        "[DDL connector] Try Applying ddl {}, current: {}",
        ddlReady.ddlVersion(),
        currentDdlVersion);

    // 다른 워커노드에서 DML 처리가 이뤄진 경우를 고려해, API 호출하여 최신 정보를 받고 CmdbResponseCache 갱신
    cmdbManager.fetchDmlConsumerStatuses(currentDdlVersion);

    // CMDB 에 기록된 DML 처리 진행상황과, 이번에 수신한 DDL 이벤트에 기록된 "기존 버전의 마지막 DML" 정보와 비교.
    // DDL 이벤트가 가리키는 파티션별 DML들이 모두 처리된것을 확인하면 DDL 적용 진행.
    boolean isProcessed =
        lastDMLInfoList.stream()
            .allMatch(
                it -> {
                  DmlStatusResponse partitionStatus =
                      cmdbManager.getLatestDmlConsumerStatus(it.getPartition());
                  return RecordUtils.isPartitionDMLProcessed(
                      it, currentDdlVersion, partitionStatus);
                });

    if (isProcessed) {
      try {
        LOG.info(
            "[DDL connector] lastProcessedDML matched with DML condition on CMDB. apply DDL: {}",
            ddlReady.ddlVersion());
        applyDDL(ddlReady.ddlVersion(), ddlReady.ddl());
      } catch (Exception e) {
        LOG.error(
            "[DDL connector] Error applying ddl: {}, ddl_version: {}",
            ddlReady.ddl(),
            ddlReady.ddlVersion(),
            e);
        alert.sendNotificationMessage(
            String.format(
                """
                                DDL 처리 중 에러발생
                                DDL: %s
                                DDL version: %s
                                """,
                ddlReady.ddl(), ddlReady.ddlVersion()),
            AlertTarget.ALL,
            Level.ERROR);
        throw new RuntimeException(e);
      }
      LOG.info("[DDL connector] Commit complete to table {}", tableIdentifier);
    } else {
      LOG.info(
          "[DDL connector] lastProcessedDML unmatched with DML condition on CMDB. skip DDL: {}",
          ddlReady.ddl());
    }
    return isProcessed;
  }

  // TODO: 실패케이스 핸들링
  private void applyDDL(long incomingDdlVersion, String ddl) {
    TranslationResponse result =
        translator.translate(
            "mysql",
            "iceberg",
            config.tables().get(0),
            ddl,
            Sets.newHashSet(DDLType.INDEX),
            convertTimezoneRequired);

    LOG.info("[DDL connector] Translator response: {}", result.message());
    Arrays.stream(result.translatedDDLs())
        .forEach(
            translatedDdl ->
                LOG.info(
                    "[DDL connector] Translated DDL - operation: {}, columnName: {}, columnOldName: {}, "
                        + "columnNewName: {}, columnDetail: {}",
                    translatedDdl.operation(),
                    translatedDdl.columnName(),
                    translatedDdl.columnOldName(),
                    translatedDdl.columnNewName(),
                    translatedDdl.columnDetail()));

    TableIdentifier tableIdentifier = TableIdentifier.parse(config.tables().get(0));
    Table table = catalog.loadTable(tableIdentifier);
    String branch = config.tableConfig(tableIdentifier.toString()).commitBranch();

    Transaction transaction = table.newTransaction();

    Arrays.stream(result.translatedDDLs())
        .forEach(
            tranlatedDdl -> {
              switch (tranlatedDdl.operation()) {
                case ADD_COLUMN:
                  UpdateSchema addColumn = transaction.updateSchema();
                  SchemaUtils.addColumn(
                      addColumn,
                      tranlatedDdl.columnName(),
                      tranlatedDdl.columnDetail().type(),
                      config);
                  addColumn.commit();
                  break;
                case DROP_COLUMN:
                  UpdateSchema dropColumn = transaction.updateSchema();
                  SchemaUtils.dropColumn(dropColumn, tranlatedDdl.columnName());
                  dropColumn.commit();
                  break;
                case MODIFY_COLUMN:
                  UpdateSchema modifyColumn = transaction.updateSchema();
                  Type currentType = table.schema().findType(tranlatedDdl.columnName());
                  SchemaUtils.modifyColumn(
                      modifyColumn,
                      tranlatedDdl.columnName(),
                      currentType,
                      tranlatedDdl.columnDetail().type());
                  modifyColumn.commit();
                  break;
                case RENAME_COLUMN:
                  UpdateSchema rename = transaction.updateSchema();
                  SchemaUtils.renameColumn(
                      rename, tranlatedDdl.columnName(), tranlatedDdl.columnNewName());
                  rename.commit();
                  break;
                case TRUNCATE:
                  DeleteFiles deletes = transaction.newDelete();
                  if (branch != null) {
                    deletes.toBranch(branch);
                  }
                  SchemaUtils.truncateTable(table, deletes, branch);
                  deletes.commit();
                  break;
                default:
                  throw new UnsupportedOperationException(
                      "Unsupported DDL type: " + tranlatedDdl.operation());
              }
            });

    transaction.commitTransaction();

    // dml_consumer_status 에 이전 ddl 버전 상태 업데이트
    updateConsumerStatusDDLVersion();

    // ddl_executions 에 ddl 처리내역 업데이트
    updateDDLProcessingStatus(
        incomingDdlVersion,
        result.translatedDDLs(),
        DdlStatus.PROCESSED,
        "success",
        table.schema());
  }

  /** ddl 처리결과를 CMDB에 기록한다 */
  public void updateDDLProcessingStatus(
      long ddlVersion, DDL[] ddls, DdlStatus status, String resultMsg, Schema sinkSchema) {
    List<DdlExecutionResponse> res = cmdbManager.getDdlExecutions(ddlVersion, DdlStatus.RECEIVED);
    if (res.isEmpty()) {
      LOG.warn("No DDL execution found for ddlVersion: {}, status: RECEIVED", ddlVersion);
      return;
    } else if (res.size() > 1) {
      LOG.warn("Multiple DDL executions found for ddlVersion: {}, status: RECEIVED", ddlVersion);
    }

    cmdbManager.patchDdlExecutionResult(
        res.get(0).getId(),
        new DdlExecutionPatchRequest(
            Arrays.toString(ddls), status, resultMsg, sinkSchema.toString()));
  }

  // 현재 ddl version 에 대한 모든 파티션의 DML consumer status 를 DONE 으로 업데이트
  public void updateConsumerStatusDDLVersion() {
    long closedDdlVersion = cmdbManager.getLatestDdlVersion();
    List<DmlStatusResponse> statuses = cmdbManager.getDmlConsumerStatuses(closedDdlVersion);

    for (DmlStatusResponse status : statuses) {
      DmlStatusPatchRequest request = new DmlStatusPatchRequest(null, null, PartitionStatus.DONE);
      cmdbManager.patchDmlConsumerStatus(status.getId(), request);
    }
  }

  private String offsetsToJson(Map<Integer, Long> offsets) {
    try {
      return MAPPER.writeValueAsString(offsets);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private void commitToTable(
      TableReference tableReference,
      List<Envelope> envelopeList,
      Map<Integer, Long> controlTopicOffsets,
      OffsetDateTime validThroughTs) {
    TableIdentifier tableIdentifier = tableReference.identifier();
    LOG.info("[DDL connector] Committing to table {}", tableIdentifier);
    Table table;
    try {
      table = catalog.loadTable(tableIdentifier);
    } catch (NoSuchTableException e) {
      LOG.warn("Table not found, skipping commit: {}", tableIdentifier, e);
      return;
    }

    if (tableReference.uuid() != null && !tableReference.uuid().equals(table.uuid())) {
      LOG.warn(
          "Skipping commits to table {} due to target table mismatch.  Expected: {} Received: {}",
          tableIdentifier,
          table.uuid(),
          tableReference.uuid());
      return;
    }

    String branch = config.tableConfig(tableIdentifier.toString()).commitBranch();

    // Control topic partition offsets may include a subset of partition ids if there were no
    // records for other partitions.  Merge the updated topic partitions with the last committed
    // offsets.
    Map<Integer, Long> committedOffsets = lastCommittedOffsetsForTable(table, branch);
    Map<Integer, Long> mergedOffsets =
        Stream.of(committedOffsets, controlTopicOffsets)
            .flatMap(map -> map.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Long::max));
    String offsetsJson = offsetsToJson(mergedOffsets);

    List<DataWritten> payloads =
        envelopeList.stream()
            .filter(
                envelope -> {
                  Long minOffset = committedOffsets.get(envelope.partition());
                  return minOffset == null || envelope.offset() >= minOffset;
                })
            .map(envelope -> (DataWritten) envelope.event().payload())
            .toList();

    List<DataFile> dataFiles =
        payloads.stream()
            .filter(payload -> payload.dataFiles() != null)
            .flatMap(payload -> payload.dataFiles().stream())
            .filter(dataFile -> dataFile.recordCount() > 0)
            .filter(distinctByKey(ContentFile::location))
            .toList();

    List<DeleteFile> deleteFiles =
        payloads.stream()
            .filter(payload -> payload.deleteFiles() != null)
            .flatMap(payload -> payload.deleteFiles().stream())
            .filter(deleteFile -> deleteFile.recordCount() > 0)
            .filter(distinctByKey(ContentFile::location))
            .toList();

    if (terminated) {
      throw new ConnectException("Coordinator is terminated, commit aborted");
    }

    if (dataFiles.isEmpty() && deleteFiles.isEmpty()) {
      LOG.info("Nothing to commit to table {}, skipping", tableIdentifier);
    } else {
      String taskId = String.format("%s-%s", config.connectorName(), config.taskId());
      if (deleteFiles.isEmpty()) {
        AppendFiles appendOp =
            table.newAppend().validateWith(offsetValidator(tableIdentifier, committedOffsets));
        if (branch != null) {
          appendOp.toBranch(branch);
        }
        appendOp.set(snapshotOffsetsProp, offsetsJson);
        appendOp.set(COMMIT_ID_SNAPSHOT_PROP, commitState.currentCommitId().toString());
        appendOp.set(TASK_ID_SNAPSHOT_PROP, taskId);
        if (validThroughTs != null) {
          appendOp.set(VALID_THROUGH_TS_SNAPSHOT_PROP, validThroughTs.toString());
        }
        dataFiles.forEach(appendOp::appendFile);
        appendOp.commit();
      } else {
        RowDelta deltaOp =
            table.newRowDelta().validateWith(offsetValidator(tableIdentifier, committedOffsets));
        if (branch != null) {
          deltaOp.toBranch(branch);
        }
        deltaOp.set(snapshotOffsetsProp, offsetsJson);
        deltaOp.set(COMMIT_ID_SNAPSHOT_PROP, commitState.currentCommitId().toString());
        deltaOp.set(TASK_ID_SNAPSHOT_PROP, taskId);
        if (validThroughTs != null) {
          deltaOp.set(VALID_THROUGH_TS_SNAPSHOT_PROP, validThroughTs.toString());
        }
        dataFiles.forEach(deltaOp::addRows);
        deleteFiles.forEach(deltaOp::addDeletes);
        deltaOp.commit();
      }

      Long snapshotId = latestSnapshot(table, branch).snapshotId();
      Event event =
          new Event(
              config.connectGroupId(),
              new CommitToTable(
                  commitState.currentCommitId(), tableReference, snapshotId, validThroughTs));
      send(event);

      LOG.info(
          "Commit complete to table {}, snapshot {}, commit ID {}, valid-through {}",
          tableIdentifier,
          snapshotId,
          commitState.currentCommitId(),
          validThroughTs);
    }
  }

  private SnapshotAncestryValidator offsetValidator(
      TableIdentifier tableIdentifier, Map<Integer, Long> expectedOffsets) {

    return new SnapshotAncestryValidator() {
      private Map<Integer, Long> lastCommittedOffsets;

      @Override
      public boolean validate(Iterable<Snapshot> baseSnapshots) {
        lastCommittedOffsets = lastCommittedOffsets(baseSnapshots);

        return expectedOffsets.equals(lastCommittedOffsets);
      }

      @Override
      public String errorMessage() {
        return String.format(
            "Cannot commit to %s, stale offsets: Expected: %s Committed: %s",
            tableIdentifier, expectedOffsets, lastCommittedOffsets);
      }
    };
  }

  private <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {
    Map<Object, Boolean> seen = Maps.newConcurrentMap();
    return t -> seen.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
  }

  private Snapshot latestSnapshot(Table table, String branch) {
    if (branch == null) {
      return table.currentSnapshot();
    }
    return table.snapshot(branch);
  }

  private Map<Integer, Long> lastCommittedOffsetsForTable(Table table, String branch) {
    Snapshot snapshot = latestSnapshot(table, branch);

    if (snapshot == null) {
      return Map.of();
    }

    Iterable<Snapshot> branchAncestry =
        SnapshotUtil.ancestorsOf(snapshot.snapshotId(), table::snapshot);
    return lastCommittedOffsets(branchAncestry);
  }

  private Map<Integer, Long> lastCommittedOffsets(Iterable<Snapshot> snapshots) {
    return Streams.stream(snapshots)
        .filter(Objects::nonNull)
        .filter(snapshot -> snapshot.summary().containsKey(snapshotOffsetsProp))
        .map(snapshot -> snapshot.summary().get(snapshotOffsetsProp))
        .map(this::parseOffsets)
        .findFirst()
        .orElseGet(Map::of);
  }

  private Map<Integer, Long> parseOffsets(String value) {
    if (value == null) {
      return Map.of();
    }

    TypeReference<Map<Integer, Long>> typeRef = new TypeReference<Map<Integer, Long>>() {};
    try {
      return MAPPER.readValue(value, typeRef);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  void terminate() {
    this.terminated = true;

    exec.shutdownNow();

    // wait for coordinator termination, else cause the sink task to fail
    try {
      if (!exec.awaitTermination(1, TimeUnit.MINUTES)) {
        throw new ConnectException("Timed out waiting for coordinator shutdown");
      }
    } catch (InterruptedException e) {
      throw new ConnectException("Interrupted while waiting for coordinator shutdown", e);
    }
  }
}
