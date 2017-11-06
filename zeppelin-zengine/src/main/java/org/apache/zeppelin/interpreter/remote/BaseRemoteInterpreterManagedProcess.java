/*
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

package org.apache.zeppelin.interpreter.remote;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteResultHandler;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.exec.environment.EnvironmentUtils;
import org.apache.zeppelin.interpreter.thrift.RemoteInterpreterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base abstract class for remote interpreter processes
 */
public abstract class BaseRemoteInterpreterManagedProcess extends RemoteInterpreterProcess
    implements ExecuteResultHandler {
  private static final Logger logger = LoggerFactory.getLogger(
      BaseRemoteInterpreterManagedProcess.class);

  protected final String interpreterRunner;
  protected final String portRange;
  private DefaultExecutor executor;
  protected ProcessLogOutputStream processOutput;
  private ExecuteWatchdog watchdog;
  protected AtomicBoolean running = new AtomicBoolean(false);
  protected String host = "localhost";
  protected int port = -1;
  protected final String interpreterDir;
  protected final String localRepoDir;
  protected final String interpreterGroupName;

  protected Map<String, String> env;

  public BaseRemoteInterpreterManagedProcess(
      String intpRunner,
      String portRange,
      String intpDir,
      String localRepoDir,
      Map<String, String> env,
      int connectTimeout,
      String interpreterGroupName) {
    super(connectTimeout);
    this.interpreterRunner = intpRunner;
    this.portRange = portRange;
    this.env = env;
    this.interpreterDir = intpDir;
    this.localRepoDir = localRepoDir;
    this.interpreterGroupName = interpreterGroupName;
  }

  @Override
  public String getHost() {
    return host;
  }

  @Override
  public int getPort() {
    return port;
  }

  protected ByteArrayOutputStream executeCommand(CommandLine cmdLine) {

    executor = new DefaultExecutor();

    ByteArrayOutputStream cmdOut = new ByteArrayOutputStream();
    processOutput = new ProcessLogOutputStream(logger);
    processOutput.setOutputStream(cmdOut);

    executor.setStreamHandler(new PumpStreamHandler(processOutput));
    watchdog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
    executor.setWatchdog(watchdog);

    try {
      Map procEnv = EnvironmentUtils.getProcEnvironment();
      procEnv.putAll(env);

      logger.info("Run interpreter process {}", cmdLine);
      executor.execute(cmdLine, procEnv, this);
    } catch (IOException e) {
      running.set(false);
      throw new RuntimeException(e);
    }

    return cmdOut;
  }

  protected abstract void stopEndPoint();

  @Override
  public abstract void start(String userName, Boolean isUserImpersonate);

  public void stop() {
    // shutdown EventPoller first.
    getRemoteInterpreterEventPoller().shutdown();
    stopEndPoint();
    if (isRunning()) {
      logger.info("kill interpreter process");
      try {
        callRemoteFunction(new RemoteFunction<Void>() {
          @Override
          public Void call(RemoteInterpreterService.Client client) throws Exception {
            client.shutdown();
            return null;
          }
        });
      } catch (Exception e) {
        logger.warn("ignore the exception when shutting down");
      }
      watchdog.destroyProcess();
    }

    executor = null;
    watchdog = null;
    running.set(false);
    logger.info("Remote process terminated");
  }

  @Override
  public void onProcessComplete(int exitValue) {
    logger.info("Interpreter process exited {}", exitValue);
    running.set(false);

  }

  @Override
  public void onProcessFailed(ExecuteException e) {
    logger.info("Interpreter process failed {}", e);
    running.set(false);
  }

  @VisibleForTesting
  public Map<String, String> getEnv() {
    return env;
  }

  @VisibleForTesting
  public String getLocalRepoDir() {
    return localRepoDir;
  }

  @VisibleForTesting
  public String getInterpreterDir() {
    return interpreterDir;
  }

  @VisibleForTesting
  public String getInterpreterGroupName() {
    return interpreterGroupName;
  }

  @VisibleForTesting
  public String getInterpreterRunner() {
    return interpreterRunner;
  }

  public boolean isRunning() {
    return running.get();
  }

  /**
   * ProcessLogOutputStream
   */
  protected static class ProcessLogOutputStream extends LogOutputStream {

    private Logger logger;
    OutputStream out;

    public ProcessLogOutputStream(Logger logger) {
      this.logger = logger;
    }

    @Override
    protected void processLine(String s, int i) {
      this.logger.debug(s);
    }

    @Override
    public void write(byte [] b) throws IOException {
      super.write(b);

      if (out != null) {
        synchronized (this) {
          if (out != null) {
            out.write(b);
          }
        }
      }
    }

    @Override
    public void write(byte [] b, int offset, int len) throws IOException {
      super.write(b, offset, len);

      if (out != null) {
        synchronized (this) {
          if (out != null) {
            out.write(b, offset, len);
          }
        }
      }
    }

    public void setOutputStream(OutputStream out) {
      synchronized (this) {
        this.out = out;
      }
    }
  }
}
