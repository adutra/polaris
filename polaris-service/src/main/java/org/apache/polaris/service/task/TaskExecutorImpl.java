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
package org.apache.polaris.service.task;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.quarkus.runtime.Startup;
import io.smallrye.common.annotation.Identifier;
import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.eclipse.microprofile.context.ManagedExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class TaskExecutorImpl implements TaskExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(TaskExecutorImpl.class);
  private static final long TASK_RETRY_DELAY = 1000;

  private final ManagedExecutor executorService;
  private final MetaStoreManagerFactory metaStoreManagerFactory;
  private final TaskFileIOSupplier fileIOSupplier;
  private final Tracer tracer;
  private final List<TaskHandler> taskHandlers = new CopyOnWriteArrayList<>();

  @Inject
  public TaskExecutorImpl(
      @Identifier("task-executor") ManagedExecutor executorService,
      MetaStoreManagerFactory metaStoreManagerFactory,
      TaskFileIOSupplier fileIOSupplier,
      Tracer tracer) {
    this.executorService = executorService;
    this.metaStoreManagerFactory = metaStoreManagerFactory;
    this.fileIOSupplier = fileIOSupplier;
    this.tracer = tracer;
  }

  @Startup
  public void init() {
    addTaskHandler(new TableCleanupTaskHandler(this, metaStoreManagerFactory, fileIOSupplier));
    addTaskHandler(
        new ManifestFileCleanupTaskHandler(
            fileIOSupplier, Executors.newVirtualThreadPerTaskExecutor()));
  }

  /**
   * Add a {@link TaskHandler}. {@link TaskEntity}s will be tested against the {@link
   * TaskHandler#canHandleTask(TaskEntity)} method and will be handled by the first handler that
   * responds true.
   */
  public void addTaskHandler(TaskHandler taskHandler) {
    taskHandlers.add(taskHandler);
  }

  /**
   * Register a {@link CallContext} for a specific task id. That task will be loaded and executed
   * asynchronously with a clone of the provided {@link CallContext}.
   */
  @Override
  public void addTaskHandlerContext(long taskEntityId, CallContext callContext) {
    // Unfortunately CallContext is a request-scoped bean and must be cloned now,
    // because its usage inside the TaskExecutor thread pool will outlive its
    // lifespan, so the original CallContext will eventually be closed while
    // the task is still running.
    CallContext clone = CallContext.copyOf(callContext);
    tryHandleTask(taskEntityId, clone, null, 1);
  }

  private @Nonnull CompletableFuture<Void> tryHandleTask(
      long taskEntityId, CallContext callContext, Throwable e, int attempt) {
    if (attempt > 3) {
      return CompletableFuture.failedFuture(e);
    }
    return executorService
        .runAsync(
            () -> {
              Span span =
                  tracer
                      .spanBuilder("task-attempt")
                      .setParent(Context.current())
                      .setAttribute("realmId", callContext.getRealmContext().getRealmIdentifier())
                      .setAttribute("taskEntityId", taskEntityId)
                      .setAttribute("attempt", attempt)
                      .startSpan();
              try (Scope ignored = span.makeCurrent()) {
                handleTask(taskEntityId, callContext);
              } finally {
                span.end();
              }
            })
        .exceptionallyComposeAsync(
            (t) -> {
              LOGGER.warn("Failed to handle task entity id {}", taskEntityId, t);
              return tryHandleTask(taskEntityId, callContext, t, attempt + 1);
            },
            CompletableFuture.delayedExecutor(
                TASK_RETRY_DELAY * (long) attempt, TimeUnit.MILLISECONDS, executorService));
  }

  private void handleTask(long taskEntityId, CallContext ctx) {
    // set the call context INSIDE the async task
    CallContext.setCurrentContext(ctx);
    LOGGER.info("Handling task entity id {}", taskEntityId);
    PolarisMetaStoreManager metaStoreManager =
        metaStoreManagerFactory.getOrCreateMetaStoreManager(ctx.getRealmContext());
    PolarisBaseEntity taskEntity =
        metaStoreManager.loadEntity(ctx.getPolarisCallContext(), 0L, taskEntityId).getEntity();
    if (!PolarisEntityType.TASK.equals(taskEntity.getType())) {
      throw new IllegalArgumentException("Provided taskId must be a task entity type");
    }
    TaskEntity task = TaskEntity.of(taskEntity);
    Optional<TaskHandler> handlerOpt =
        taskHandlers.stream().filter(th -> th.canHandleTask(task)).findFirst();
    if (handlerOpt.isEmpty()) {
      LOGGER
          .atWarn()
          .addKeyValue("taskEntityId", taskEntityId)
          .addKeyValue("taskType", task.getTaskType())
          .log("Unable to find handler for task type");
      return;
    }
    TaskHandler handler = handlerOpt.get();
    boolean success = handler.handleTask(task);
    if (success) {
      LOGGER
          .atInfo()
          .addKeyValue("taskEntityId", taskEntityId)
          .addKeyValue("handlerClass", handler.getClass())
          .log("Task successfully handled");
      metaStoreManager.dropEntityIfExists(
          ctx.getPolarisCallContext(), null, PolarisEntity.toCore(taskEntity), Map.of(), false);
    } else {
      LOGGER
          .atWarn()
          .addKeyValue("taskEntityId", taskEntityId)
          .addKeyValue("taskEntityName", taskEntity.getName())
          .log("Unable to execute async task");
    }
  }
}
