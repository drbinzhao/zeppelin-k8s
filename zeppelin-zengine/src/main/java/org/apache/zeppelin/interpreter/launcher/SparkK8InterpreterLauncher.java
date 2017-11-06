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


package org.apache.zeppelin.interpreter.launcher;

import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.interpreter.InterpreterRunner;
import org.apache.zeppelin.interpreter.remote.SparkK8RemoteInterpreterManagedProcess;

import java.util.Map;

/**
 * Interpreter Launcher which use shell script to launch Spark interpreter process,
 * on Kubernetes cluster.
 */
public class SparkK8InterpreterLauncher extends SparkInterpreterLauncher {

  public SparkK8InterpreterLauncher(ZeppelinConfiguration zConf) {
    super(zConf);
  }

  @Override
  protected InterpreterClient createRemoteProcess(InterpreterLaunchContext context,
                                                  InterpreterRunner runner,
                                                  String groupName,
                                                  int connectTimeout) {
    // create new remote process
    String localRepoPath = zConf.getInterpreterLocalRepoPath() + "/"
            + context.getInterpreterGroupId();
    return new SparkK8RemoteInterpreterManagedProcess(
            runner != null ? runner.getPath() : zConf.getInterpreterRemoteRunnerPath(),
            zConf.getCallbackPortRange(),
            zConf.getInterpreterDir() + "/" + groupName, localRepoPath,
            buildEnvFromProperties(), connectTimeout, groupName, context.getInterpreterGroupId());
  }

  @Override
  protected Map<String, String> buildEnvFromProperties() {
    Map<String, String> env = super.buildEnvFromProperties();
    env.put("RUN_SPARK_ON_K8", Boolean.TRUE.toString());
    return env;
  }

}
