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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;

/**
 * This class manages start / stop of Spark remote interpreter process on a Kubernetes cluster.
 * After Spark Driver started by spark-submit is in Running state, creates a Kubernetes service
 * to connect to RemoteInterpreterServer running inside Spark Driver.
 */
public class SparkK8RemoteInterpreterManagedProcess extends BaseRemoteInterpreterManagedProcess {

  private static final Logger logger = LoggerFactory.getLogger(
      SparkK8RemoteInterpreterManagedProcess.class);

  public static final String SPARK_APP_SELECTOR = "spark-app-selector";
  public static final String DRIVER_SERVICE_NAME_SUFFIX = "-ri-svc";
  public static final String KUBERNETES_NAMESPACE = "default";
  public static final String DRIVER_POD_NAME_PREFIX = "zri-";
  public static final String INTERPRETER_PROCESS_ID = "interpreter-processId";

  /**
   * Default url for Kubernetes inside of an Kubernetes cluster.
   */
  private static String K8_URL = "https://kubernetes:443";
  private KubernetesClient kubernetesClient;
  private String driverPodName;
  private Service driverService;
  private String interpreterGroupId;
  private String processLabelId;

  public SparkK8RemoteInterpreterManagedProcess(String intpRunner,
                                                String portRange,
                                                String intpDir,
                                                String localRepoDir,
                                                Map<String, String> env,
                                                int connectTimeout,
                                                String groupName, String interpreterGroupId) {

    super(intpRunner, portRange, intpDir, localRepoDir, env, connectTimeout, groupName);
    this.processLabelId = generatePodLabelId(interpreterGroupId);
    this.interpreterGroupId = formatId(interpreterGroupId, 50);
    this.port = 30000;
  }

  /**
   * Id for spark submit must be formatted to contain only alfanumeric chars.
   * @param str
   * @param maxLength
   * @return
   */
  private String formatId(String str, int maxLength) {
    str = str.replaceAll("[^a-zA-Z0-9]", "_").toLowerCase();
    if (str.length() > maxLength) {
      str = str.substring(0, maxLength - 1);
    }
    return str;
  }

  private String generatePodLabelId(String interpreterGroupId ) {
    return formatId(interpreterGroupId + "_" + System.currentTimeMillis(), 64);
  }

  @Override
  public void start(String userName, Boolean isUserImpersonate) {
    CommandLine cmdLine = CommandLine.parse(interpreterRunner);
    cmdLine.addArgument("-d", false);
    cmdLine.addArgument(interpreterDir, false);
    cmdLine.addArgument("-p", false);
    cmdLine.addArgument(Integer.toString(port), false);
    if (isUserImpersonate && !userName.equals("anonymous")) {
      cmdLine.addArgument("-u", false);
      cmdLine.addArgument(userName, false);
    }
    cmdLine.addArgument("-l", false);
    cmdLine.addArgument(localRepoDir, false);
    cmdLine.addArgument("-g", false);
    cmdLine.addArgument(interpreterGroupName, false);

    if (interpreterGroupId != null) {
      cmdLine.addArgument("-i", false);
      cmdLine.addArgument(interpreterGroupId, false);
    }
    if (processLabelId != null) {
      cmdLine.addArgument("-t", false);
      cmdLine.addArgument(processLabelId, false);
    }


    ByteArrayOutputStream cmdOut = executeCommand(cmdLine);

    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime < getConnectTimeout()) {
      host = obtainEndpointHost();
      try {
        if (host != null && RemoteInterpreterUtils.checkIfRemoteEndpointAccessible(host, port)) {
          running.set(true);
          break;
        } else {
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            logger.error("Exception in RemoteInterpreterProcess while synchronized reference " +
                    "Thread.sleep", e);
          }
        }
      } catch (Exception e) {
        if (logger.isDebugEnabled()) {
          logger.debug("Remote interpreter not yet accessible at {}:{}", host, port);
        }
      }
    }

    if (!running.get()) {
      throw new RuntimeException("Unable to start SparkK8RemoteInterpreterManagedProcess");
    }
  }

  protected String obtainEndpointHost() {
    String hostName = null;
    // try to obtain endpoint url from Spark driver
    try (final KubernetesClient client = getKubernetesClient()) {
      Pod driverPod = getSparkDriverPod(client, DRIVER_POD_NAME_PREFIX + interpreterGroupName);
      if (driverPod != null) {
        driverPodName = driverPod.getMetadata().getName();
        logger.debug("Driver pod name: " + driverPodName);
        Service driverService  = getOrCreateEndpointService(client, driverPod);
        if (driverService != null) {
          logger.info("ClusterIP {}", driverService.getSpec().getClusterIP());
          hostName = driverService.getSpec().getClusterIP();
        }
      }
    } catch (KubernetesClientException e) {
      logger.error(e.getMessage(), e);
    }
    return hostName;
  }

  private KubernetesClient getKubernetesClient() {
    if (kubernetesClient == null) {
      Config config = new ConfigBuilder().withMasterUrl(K8_URL).build();
      logger.info("Connect to Kubernetes cluster at: {}", K8_URL);
      kubernetesClient = new DefaultKubernetesClient(config);
    }
    return kubernetesClient;
  }

  private Pod getSparkDriverPod(KubernetesClient client, String podLabel)
      throws KubernetesClientException {

    List<Pod> podList = client.pods().inNamespace(KUBERNETES_NAMESPACE)
     .withLabel(INTERPRETER_PROCESS_ID, processLabelId).list().getItems();
    if (podList.size() >= 1) {
      for (Pod remoteServerPod : podList) {
        String podName = remoteServerPod.getMetadata().getName();
        if (podName != null && podName.startsWith(podLabel)) {
          logger.debug("Driver pod found. Status: " + remoteServerPod.getStatus().getPhase());
          if (remoteServerPod.getStatus().getPhase().equalsIgnoreCase("running")) {
            return remoteServerPod;
          }
        }
      }
    } else {
      logger.debug("Pod not found!");
    }

    return null;
  }

  private Service getEndpointService(KubernetesClient client, String serviceName)
      throws KubernetesClientException {
    logger.debug("Check if RemoteInterpreterServer service {} exists", serviceName);
    return client.services().inNamespace(KUBERNETES_NAMESPACE).withName(serviceName).get();
  }

  private Service getOrCreateEndpointService(KubernetesClient client, Pod driverPod)
      throws KubernetesClientException {
    String serviceName = driverPodName + DRIVER_SERVICE_NAME_SUFFIX;
    driverService = getEndpointService(client, serviceName);

    // create endpoint service for RemoteInterpreterServer
    if (driverService == null) {
      Map<String, String> labels = driverPod.getMetadata().getLabels();
      String label = labels.get(SPARK_APP_SELECTOR);
      logger.info("Create RemoteInterpreterServer service for spark-app-selector: {}", label);
      driverService = new ServiceBuilder().withNewMetadata()
              .withName(serviceName).endMetadata()
              .withNewSpec().addNewPort().withProtocol("TCP")
              .withPort(getPort()).withNewTargetPort(getPort()).endPort()
              .addToSelector(SPARK_APP_SELECTOR, label)
              .withType("ClusterIP")
              .endSpec().build();
      driverService = client.services().inNamespace(KUBERNETES_NAMESPACE).create(driverService);
    }

    return driverService;
  }

  private void deleteEndpointService(KubernetesClient client)
      throws KubernetesClientException {
    boolean result = client.services().inNamespace(KUBERNETES_NAMESPACE).delete(driverService);
    logger.info("Delete RemoteInterpreterServer service {} : {}",
      driverService.getMetadata().getName(), result);
  }

  @Override
  protected void stopEndPoint() {
    if (driverPodName != null) {
      try (KubernetesClient client = getKubernetesClient()) {
        deleteEndpointService(client);
        client.close();
        kubernetesClient = null;
      } catch (KubernetesClientException e) {
        logger.error(e.getMessage(), e);
      }
    }
  }

}
