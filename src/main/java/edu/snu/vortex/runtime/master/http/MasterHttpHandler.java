/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.vortex.runtime.master.http;

import edu.snu.vortex.runtime.exception.ExecutorNotFoundException;
import edu.snu.vortex.runtime.master.RuntimeMaster;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.webserver.HttpHandler;
import org.apache.reef.webserver.ParsedHttpRequest;

import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Handles HTTP requests sent to the Vortex Master.
 */
public final class MasterHttpHandler implements HttpHandler {
  private static final String KEY_EXECUTOR_ID = "executor-id";

  private String uriSpecification = "vortex";
  private final InjectionFuture<RuntimeMaster> runtimeMaster;

  @Inject
  private MasterHttpHandler(final InjectionFuture<RuntimeMaster> runtimeMaster) {
    this.runtimeMaster = runtimeMaster;
  }

  @Override
  public String getUriSpecification() {
    return uriSpecification;
  }

  @Override
  public void setUriSpecification(final String s) {
    uriSpecification = s;
  }

  @Override
  public void onHttpRequest(final ParsedHttpRequest request, final HttpServletResponse response)
      throws IOException, ServletException {
    final String target = request.getTargetEntity().toLowerCase();
    final Map<String, List<String>> queryMap = request.getQueryMap();
    System.out.println("QueryMap" + queryMap.keySet().toString() + queryMap.values().toString());

    final Response result;
    switch (target) {
    case "job-state":
      result = onJobState();
      break;
    case "executors":
      result = onExecutors();
      break;
    case "task-groups":
      if (queryMap.isEmpty()) {
        result = Response.badRequest(String.format("The POST request should specify %s.", KEY_EXECUTOR_ID));
      } else {
        result = onTaskGroups(queryMap);
      }
      break;
    default:
      result = Response.badRequest("Not implemented yet");
    }

    // Send response to the http client
    final int status = result.getStatus();
    final String message = result.getMessage();

    if (result.isOK()) {
      response.getOutputStream().println(message);
    } else {
      response.sendError(status, message);
    }
  }

  private Response onExecutors() {
    return Response.ok(runtimeMaster.get().getExecutorsState());
  }

  private Response onTaskGroups(final Map<String, List<String>> queryMap) {
    final List<String> args = queryMap.get(KEY_EXECUTOR_ID);
    if (args.size() != 1) {
      return Response.badRequest(String.format("Usage : only one %s at a time", KEY_EXECUTOR_ID));
    }

    final String executorId = args.get(0);
    try {
      return Response.ok(runtimeMaster.get().getRunningTaskGroups(executorId));
    } catch (final ExecutorNotFoundException e) {
      return Response.notFound(executorId);
    }
  }

  private Response onJobState() {
    return Response.ok(runtimeMaster.get().getJobState());
  }
}