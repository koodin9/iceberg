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
package org.apache.iceberg.connect.auth;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.PrivilegedExceptionAction;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.common.Alert;
import org.apache.iceberg.connect.common.AlertTarget;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

public class KerberosAuthManager {
  private static final Logger LOG = LoggerFactory.getLogger(KerberosAuthManager.class);

  private static volatile KerberosAuthManager INSTANCE;
  private static final Object LOCK = new Object();

  private UserGroupInformation kerberosUGI;
  private final ScheduledExecutorService renewalScheduler;
  private final AtomicBoolean isInitialized = new AtomicBoolean(false);
  private final AtomicLong lastRenewalTime = new AtomicLong(0);
  private final Alert alert;

  private final String principal;
  private final String keytabPath;
  private final long renewalIntervalMinutes;

  private KerberosAuthManager(IcebergSinkConfig config) {
    LOG.info("Initializing KerberosAuthManager");

    this.alert = new Alert(config);
    this.principal = config.connectHdfsPrincipal();
    this.keytabPath = config.connectHdfsKeytab();
    this.renewalIntervalMinutes = config.kerberosTicketRenewPeriodMinutes();

    // 하둡 설정 및 ugi 초기화
    UserGroupInformation.reset();

    // Kerberos 로그인
    this.kerberosUGI = simpleLogin();
    UserGroupInformation.setLoginUser(this.kerberosUGI);

    // 자동 갱신 스케줄러 초기화
    this.renewalScheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread thread = new Thread(r, "kerberos-renewal-thread");
              thread.setDaemon(true);
              return thread;
            });

    // 초기 TGT 체크/갱신
    performInitialAuthentication();

    // 자동 갱신 시작
    startRenewalScheduler();

    // shutdown hook 등록
    registerShutdownHook();

    this.isInitialized.set(true);
    LOG.info("KerberosAuthManager initialized for principal: {}", principal);
  }

  /**
   * 최초 호출로 인스턴스가 생성되고 나면, 동일 워커의 전체 커넥터 task 에서 같은 인스턴스를 사용하게 된다. 따라서 kerberos 관련 설정은 커넥터별 수정이 불가능.
   * 변경시 전체 커넥터에서 수정되어야 한다.
   */
  public static KerberosAuthManager getInstance(IcebergSinkConfig config) {
    if (INSTANCE == null) {
      synchronized (LOCK) {
        if (INSTANCE == null) {
          INSTANCE = new KerberosAuthManager(config);
        }
      }
    }

    LOG.info("Returning KerberosAuthManager INSTANCE for principal: {}", INSTANCE.principal);
    return INSTANCE;
  }

  private void performInitialAuthentication() {
    try {
      this.kerberosUGI.checkTGTAndReloginFromKeytab();
      LOG.info(
          "Successfully performed initial Kerberos authentication for principal: {}", principal);
    } catch (IOException e) {
      String errorMessage = "Error checking TGT and relogin from keytab: " + e.getMessage();
      alert.sendNotificationMessage(errorMessage, AlertTarget.MANAGER, Level.ERROR);
      throw new UncheckedIOException(
          "Error checking TGT and relogin from keytab: " + e.getMessage(), e);
    }
  }

  private void startRenewalScheduler() {
    TimeUnit minutesUnit = TimeUnit.MINUTES;
    int initialDelay = 60;

    renewalScheduler.scheduleAtFixedRate(
        () -> {
          try {
            LOG.info("Renewing Kerberos ticket for user {}", principal);
            renewTicket();
          } catch (Exception e) {
            // 예외 발생해도 스레드 종료되지 않도록 처리
            LOG.error("Unexpected error during Kerberos ticket renewal", e);
          }
        },
        initialDelay,
        renewalIntervalMinutes,
        minutesUnit);

    LOG.info(
        "Scheduling Kerberos ticket renewal every {} {}. Initial delay: {} {}",
        renewalIntervalMinutes,
        minutesUnit,
        initialDelay,
        minutesUnit);
  }

  /** Kerberos 티켓 갱신 TGT 갱신이 불가능한 경우 keytab으로부터 새로 로그인 시도 */
  public synchronized boolean renewTicket() {
    if (!isInitialized.get()) {
      LOG.warn("KerberosAuthManager is not initialized.");
      return false;
    }

    try {
      // 먼저 일반적인 TGT 갱신 시도
      kerberosUGI.checkTGTAndReloginFromKeytab();
      lastRenewalTime.set(System.currentTimeMillis());

      printAuthStatus();
      return true;
    } catch (IOException e) {
      LOG.warn("Failed to renew kerberos ticket. Attempting fresh login from keytab..", e);

      try {
        // TGT 갱신 실패 시 keytab으로부터 완전히 새로운 로그인 시도
        performFreshLogin();
        lastRenewalTime.set(System.currentTimeMillis());

        printAuthStatus();
        return true;
      } catch (IOException freshLoginException) {
        LOG.error(
            "Kerberos 티켓 갱신 error: {}, Fresh login error: {}",
            e.getMessage(),
            freshLoginException.getMessage(),
            freshLoginException);
        alert.sendNotificationMessage(
            String.format(
                "Kerberos 티켓 갱신 error: %s, Fresh login error: %s",
                e.getMessage(), freshLoginException.getMessage()),
            AlertTarget.ALL,
            Level.ERROR);
        return false;
      }
    }
  }

  /** keytab 을 이용해 새 로그인 수행 */
  private void performFreshLogin() throws IOException {
    LOG.info("New Kerberos login with keytab: principal={}, keytab={}", principal, keytabPath);

    try {
      kerberosUGI.logoutUserFromKeytab();
    } catch (Exception e) {
      LOG.debug("Exception during logout, ignoring: {}", e.getMessage(), e);
    }

    kerberosUGI = simpleLogin();

    LOG.info("New Kerberos login successful for principal: {}", principal);
  }

  /**
   * Kerberos 인증 컨텍스트에서 작업 실행 PrivilegedExceptionAction을 사용하여 checked exception을 명확하게 전파
   *
   * @param action 실행할 작업
   * @param <T> 작업의 반환 타입
   * @return 작업의 결과
   * @throws RuntimeException 작업 실행 중 발생한 예외 (원래 예외 타입 최대한 보존)
   */
  public <T> T executeWithAuth(PrivilegedExceptionAction<T> action) {
    ensureInitialized();

    try {
      kerberosUGI.checkTGTAndReloginFromKeytab();
    } catch (IOException e) {
      LOG.error("Kerberos TGT check/renewal failed", e);
      throw new UncheckedIOException("Kerberos TGT check/renewal failed", e);
    }

    try {
      return kerberosUGI.doAs(action);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Action interrupted", e);
    } catch (RuntimeException e) {
      // RuntimeException은 그대로 던짐 (RebalanceInProgressException 등)
      throw e;
    } catch (Exception e) {
      // Checked exception (PrivilegedActionException 등)은 cause를 unwrap, 시스템 레벨 Error는 그대로 던짐
      Throwable cause = e.getCause();
      if (cause instanceof Error) {
        throw (Error) cause;
      }
      throw new RuntimeException("Action failed", e);
    }
  }

  /** 인증 상태정보 반환 */
  public void printAuthStatus() {
    if (!isInitialized.get()) {
      LOG.warn("KerberosAuthManager is not initialized.");
      return;
    }

    try {
      LOG.info(
          "Principal: {}, Keytab: {}, Has Kerberos Credentials: {}, Should Relogin: {}, Last Renewal: {}",
          principal,
          keytabPath,
          kerberosUGI.hasKerberosCredentials(),
          kerberosUGI.shouldRelogin(),
          lastRenewalTime.get() > 0 ? Instant.ofEpochMilli(lastRenewalTime.get()) : "none");
    } catch (Exception e) {
      LOG.error("Failed to log status: {}", e.getMessage(), e);
    }
  }

  public void shutdown() {
    LOG.info("Kerberos scheduler is shutting down...");

    if (renewalScheduler != null && !renewalScheduler.isShutdown()) {
      renewalScheduler.shutdown();
      try {
        if (!renewalScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
          LOG.warn("Kerberos scheduler did not terminated in the specified time.");
          renewalScheduler.shutdownNow();
        }
      } catch (InterruptedException e) {
        LOG.error("Kerberos scheduler shutdown interrupted.", e);
        renewalScheduler.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }

    isInitialized.set(false);
    LOG.info("KerberosAuthManager shutdown complete.");
  }

  private void ensureInitialized() {
    if (!isInitialized.get()) {
      throw new IllegalStateException("KerberosAuthManager is not initialized.");
    }
  }

  @SuppressWarnings("ShutdownHook")
  private void registerShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown, "kerberos-shutdown-hook"));
  }

  /**
   * 기존 사용하던 Utilities.kerberosLogin() 는, 로드되는 hadoop config file 들의 설정에 문제가 없음에도 불구하고 지정한 principal
   * 의 KERBEROS ugi 가 아니라 root 유저에 대한 SIMPLE 인증 ugi 가 반환되는 문제가 있음. 이에 따라 원인이 밝혀질 때 까지는 필수 설정만 직접
   * 지정한다
   */
  private UserGroupInformation simpleLogin() {
    LOG.info("Attempting Kerberos login: principal={}, keytab={}", principal, keytabPath);

    if (principal == null || keytabPath == null) {
      throw new ConfigException(
          "Hadoop is using Kerberos for authentication, you need to provide both a connect "
              + "principal and the path to the keytab of the principal.");
    }

    Configuration cfg = new Configuration();
    cfg.set("hadoop.security.authentication", "kerberos");
    cfg.set("hadoop.security.authorization", "true");
    UserGroupInformation.setConfiguration(cfg);

    try {
      UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

      LOG.info("Kerberos login result:");
      LOG.info("  - User: {}", ugi.getUserName());
      LOG.info("  - Has Kerberos Credentials: {}", ugi.hasKerberosCredentials());
      LOG.info("  - Authentication Method: {}", ugi.getAuthenticationMethod());

      return ugi;
    } catch (IOException e) {
      LOG.error("Kerberos login failed", e);
      throw new UncheckedIOException("Could not authenticate with Kerberos: " + e.getMessage(), e);
    }
  }
}
