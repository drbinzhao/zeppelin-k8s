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

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.ExecuteResultHandler;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.zeppelin.interpreter.thrift.CallbackInfo;
import org.apache.zeppelin.interpreter.thrift.RemoteInterpreterCallbackService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * This class manages start / stop of remote interpreter process
 */
public class RemoteInterpreterManagedProcess extends BaseRemoteInterpreterManagedProcess
    implements ExecuteResultHandler {
  private static final Logger logger = LoggerFactory.getLogger(
      RemoteInterpreterManagedProcess.class);

  TServer callbackServer;

  public RemoteInterpreterManagedProcess(
      String intpRunner,
      String portRange,
      String intpDir,
      String localRepoDir,
      Map<String, String> env,
      int connectTimeout,
      String interpreterGroupName) {
    super(intpRunner, portRange, intpDir, localRepoDir, env, connectTimeout, interpreterGroupName);
  }

  @Override
  public void start(String userName, Boolean isUserImpersonate) {
    // start server process
    final String callbackHost;
    final int callbackPort;
    try {
      port = RemoteInterpreterUtils.findRandomAvailablePortOnAllLocalInterfaces(portRange);
      logger.info("Choose port {} for RemoteInterpreterProcess", port);
      callbackHost = RemoteInterpreterUtils.findAvailableHostAddress();
      callbackPort = RemoteInterpreterUtils.findRandomAvailablePortOnAllLocalInterfaces();
    } catch (IOException e1) {
      throw new RuntimeException(e1);
    }

    logger.info("Thrift server for callback will start. Port: {}", callbackPort);
    try {
      callbackServer = new TThreadPoolServer(
        new TThreadPoolServer.Args(new TServerSocket(callbackPort)).processor(
          new RemoteInterpreterCallbackService.Processor<>(
            new RemoteInterpreterCallbackService.Iface() {
              @Override
              public void callback(CallbackInfo callbackInfo) throws TException {
                logger.info("Registered: {}", callbackInfo);
                host = callbackInfo.getHost();
                port = callbackInfo.getPort();
                running.set(true);
                synchronized (running) {
                  running.notify();
                }
              }
            })));
      // Start thrift server to receive callbackInfo from RemoteInterpreterServer;
      new Thread(new Runnable() {
        @Override
        public void run() {
          callbackServer.serve();
        }
      }).start();

      Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
        @Override
        public void run() {
          if (callbackServer.isServing()) {
            callbackServer.stop();
          }
        }
      }));

      while (!callbackServer.isServing()) {
        logger.debug("callbackServer is not serving");
        Thread.sleep(500);
      }
      logger.debug("callbackServer is serving now");
    } catch (TTransportException e) {
      logger.error("callback server error.", e);
    } catch (InterruptedException e) {
      logger.warn("", e);
    }

    CommandLine cmdLine = CommandLine.parse(interpreterRunner);
    cmdLine.addArgument("-d", false);
    cmdLine.addArgument(interpreterDir, false);
    cmdLine.addArgument("-c", false);
    cmdLine.addArgument(callbackHost, false);
    cmdLine.addArgument("-p", false);
    cmdLine.addArgument(Integer.toString(callbackPort), false);
    if (isUserImpersonate && !userName.equals("anonymous")) {
      cmdLine.addArgument("-u", false);
      cmdLine.addArgument(userName, false);
    }
    cmdLine.addArgument("-l", false);
    cmdLine.addArgument(localRepoDir, false);
    cmdLine.addArgument("-g", false);
    cmdLine.addArgument(interpreterGroupName, false);

    ByteArrayOutputStream cmdOut = executeCommand(cmdLine);

    try {
      synchronized (running) {
        if (!running.get()) {
          running.wait(getConnectTimeout() * 2);
        }
      }
      if (!running.get()) {
        stopEndPoint();
        throw new RuntimeException(new String(cmdOut.toByteArray()));
      }
    } catch (InterruptedException e) {
      logger.error("Remote interpreter is not accessible");
    }
    processOutput.setOutputStream(null);
  }

  @Override
  protected void stopEndPoint() {
    callbackServer.stop();
  }

}
