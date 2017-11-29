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
package edu.snu.onyx.runtime.executor;

import edu.snu.onyx.common.ContextImpl;
import edu.snu.onyx.common.exception.PartitionFetchException;
import edu.snu.onyx.common.exception.PartitionWriteException;
import edu.snu.onyx.common.ir.Reader;
import edu.snu.onyx.common.ir.Transform;
import edu.snu.onyx.common.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.common.ir.vertex.OperatorVertex;
import edu.snu.onyx.runtime.common.plan.RuntimeEdge;
import edu.snu.onyx.runtime.common.plan.physical.*;
import edu.snu.onyx.runtime.common.state.TaskGroupState;
import edu.snu.onyx.runtime.executor.data.PartitionManagerWorker;
import edu.snu.onyx.runtime.executor.datatransfer.DataTransferFactory;
import edu.snu.onyx.runtime.executor.datatransfer.InputReader;
import edu.snu.onyx.runtime.executor.datatransfer.OutputCollectorImpl;
import edu.snu.onyx.runtime.executor.datatransfer.OutputWriter;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Executes a task group.
 */
public final class TaskGroupExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(TaskGroupExecutor.class.getName());

  private final TaskGroup taskGroup;
  private final TaskGroupStateManager taskGroupStateManager;
  private final List<PhysicalStageEdge> stageIncomingEdges;
  private final List<PhysicalStageEdge> stageOutgoingEdges;
  private final DataTransferFactory channelFactory;
  private final PartitionManagerWorker partitionManagerWorker;
  // For intra-TaskGroup element-wise data transfer.
  // Inter-Task data are transferred via OutputCollector of each Task.
  private final Map<String, OutputCollectorImpl> taskIdToLocalWriterMap;
  private final Map<String, List<OutputCollectorImpl>> taskIdToLocalReaderMap;
  private Map<String, String> taskTypeToTaskIdMap;
  private final AtomicInteger sourceParallelism;
  private final AtomicInteger numOfInputStream;
  private boolean hasBoundedSourceTask;

  /**
   * Map of task IDs in this task group to their readers/writers.
   */
  private final Map<String, List<InputReader>> taskIdToInputReaderMap;
  private final Map<String, List<OutputWriter>> taskIdToOutputWriterMap;

  private boolean isExecutionRequested;

  public TaskGroupExecutor(final TaskGroup taskGroup,
                           final TaskGroupStateManager taskGroupStateManager,
                           final List<PhysicalStageEdge> stageIncomingEdges,
                           final List<PhysicalStageEdge> stageOutgoingEdges,
                           final DataTransferFactory channelFactory,
                           final PartitionManagerWorker partitionManagerWorker) {
    this.taskGroup = taskGroup;
    this.taskGroupStateManager = taskGroupStateManager;
    this.stageIncomingEdges = stageIncomingEdges;
    this.stageOutgoingEdges = stageOutgoingEdges;
    this.channelFactory = channelFactory;
    this.partitionManagerWorker = partitionManagerWorker;

    this.hasBoundedSourceTask = false;
    this.sourceParallelism = new AtomicInteger(0);
    this.numOfInputStream = new AtomicInteger(0);

    this.taskIdToLocalWriterMap = new HashMap<>();
    this.taskIdToLocalReaderMap = new HashMap<>();
    this.taskTypeToTaskIdMap = new HashMap<>();

    this.taskIdToInputReaderMap = new HashMap<>();
    this.taskIdToOutputWriterMap = new HashMap<>();

    this.isExecutionRequested = false;

    initializeTaskGroupExecution();
  }

  /**
   * Initializes readers and writers depending on the execution properties.
   * Reader and writer exist per TaskGroup, so we consider only cross-stage edges.
   */
  private void initializeTaskGroupExecution() {
    taskGroup.getTaskDAG().topologicalDo((task -> {
      final Set<PhysicalStageEdge> inEdgesFromOtherStages = getInEdgesFromOtherStages(task);
      final Set<PhysicalStageEdge> outEdgesToOtherStages = getOutEdgesToOtherStages(task);

      // Add InputReaders for inter-stage data transfer
      inEdgesFromOtherStages.forEach(physicalStageEdge -> {
        final InputReader inputReader = channelFactory.createReader(
            task, physicalStageEdge.getSrcVertex(), physicalStageEdge);
        addInputReader(task, inputReader);
        LOG.info("log: Added InputReader, {} {} {}", taskGroup.getTaskGroupId(),
            task.getId(), task.getRuntimeVertexId());
        sourceParallelism.getAndAdd(physicalStageEdge.getSrcVertex().getProperty(ExecutionProperty.Key.Parallelism));
      });

      // Add OutputWriters for inter-stage data transfer
      outEdgesToOtherStages.forEach(physicalStageEdge -> {
        LOG.info("log: Added OutputWriter {} {} {}", taskGroup.getTaskGroupId(),
            task.getId(), task.getRuntimeVertexId());
        final OutputWriter outputWriter = channelFactory.createWriter(
            task, physicalStageEdge.getDstVertex(), physicalStageEdge);
        addOutputWriter(task, outputWriter);
      });

      // Add OutputCollectors for inter- and intra-stage data transfer
      addLocalWriter(task);

      LOG.info("log: {} getIncomingEdgesOf({})({}): {}", taskGroup.getTaskGroupId(),
          task.getId(), task.getRuntimeVertexId(),
          taskGroup.getTaskDAG().getIncomingEdgesOf(task));

      // Add LocalReaders for intra-stage data transfer
      if (!taskGroup.getTaskDAG().getIncomingEdgesOf(task).isEmpty()
          && !hasInputReader(task)) {
        addLocalReaders(task);
      }

      // Add initial Task states
      LOG.info("log: taskTypeToTaskIdMap: Adding {} {}:{}",
          taskGroup.getTaskGroupId(), task.getClass().getSimpleName(), task.getId());
      taskTypeToTaskIdMap.put(task.getId(), task.getClass().getName());
    }));

    if (sourceParallelism.get() == 0) {
      sourceParallelism.getAndAdd(1);
    }

    LOG.info("log: {} End of initTGExec", taskGroup.getTaskGroupId());
  }

  // Helper functions to initializes cross-stage edges.
  private Set<PhysicalStageEdge> getInEdgesFromOtherStages(final Task task) {
    return stageIncomingEdges.stream().filter(
        stageInEdge -> stageInEdge.getDstVertex().getId().equals(task.getRuntimeVertexId()))
        .collect(Collectors.toSet());
  }

  private Set<PhysicalStageEdge> getOutEdgesToOtherStages(final Task task) {
    return stageOutgoingEdges.stream().filter(
        stageInEdge -> stageInEdge.getSrcVertex().getId().equals(task.getRuntimeVertexId()))
        .collect(Collectors.toSet());
  }

  // Helper functions to add the initialized reader/writer/inter-Task data storage to the maintained map.
  private void addInputReader(final Task task, final InputReader inputReader) {
    taskIdToInputReaderMap.computeIfAbsent(task.getId(), readerList -> new ArrayList<>());
    taskIdToInputReaderMap.get(task.getId()).add(inputReader);
  }

  private void addOutputWriter(final Task task, final OutputWriter outputWriter) {
    taskIdToOutputWriterMap.computeIfAbsent(task.getId(), writerList -> new ArrayList<>());
    taskIdToOutputWriterMap.get(task.getId()).add(outputWriter);
  }

  private void addLocalWriter(final Task task) {
    taskIdToLocalWriterMap.put(task.getId(), new OutputCollectorImpl());
  }

  // Create a map of Task and its parent Task's OutputCollectorImpls,
  // which are the Task's LocalReaders(intra-TaskGroup readers).
  private void addLocalReaders(final Task task) {
    List<OutputCollectorImpl> localReaders = new ArrayList<>();
    List<Task> parentTasks = taskGroup.getTaskDAG().getParents(task.getId());

    if (parentTasks != null) {
      LOG.info("log: Adding LocalReader, {} {} {}", taskGroup.getTaskGroupId(),
          task.getId(), task.getRuntimeVertexId());
      parentTasks.forEach(parent -> LOG.info("log: Parents of {} {}: {}", taskGroup.getTaskGroupId(),
          task.getRuntimeVertexId(), parent.getRuntimeVertexId()));

      parentTasks.forEach(parentTask -> {
        localReaders.add(taskIdToLocalWriterMap.get(parentTask.getId()));
      });

      taskIdToLocalReaderMap.put(task.getId(), localReaders);
    } else {
      taskIdToLocalReaderMap.put(task.getId(), null);
    }
  }

  private boolean hasInputReader(final Task task) {
    return taskIdToInputReaderMap.containsKey(task.getId());
  }

  private boolean hasOutputWriter(final Task task) {
    return taskIdToOutputWriterMap.containsKey(task.getId());
  }

  /**
   * Executes the task group.
   */
  public void execute() {
    LOG.info("log: {} Executing!", taskGroup.getTaskGroupId());
    if (isExecutionRequested) {
      throw new RuntimeException("TaskGroup {" + taskGroup.getTaskGroupId() + "} execution called again!");
    } else {
      isExecutionRequested = true;
    }

    taskGroupStateManager.onTaskGroupStateChanged(TaskGroupState.State.EXECUTING, Optional.empty(), Optional.empty());

    while (!isTaskGroupComplete()) {
      taskGroup.getTaskDAG().topologicalDo(task -> {
        try {
          if (task instanceof BoundedSourceTask) {
            launchBoundedSourceTask((BoundedSourceTask) task);
          } else if (task instanceof OperatorTask) {
            launchOperatorTask((OperatorTask) task);
          } else if (task instanceof MetricCollectionBarrierTask) {
            launchMetricCollectionBarrierTask((MetricCollectionBarrierTask) task);
          } else {
            throw new UnsupportedOperationException(task.toString());
          }
        } catch (final PartitionFetchException ex) {
          taskGroupStateManager.onTaskGroupStateChanged(TaskGroupState.State.FAILED_RECOVERABLE,
              Optional.empty(),
              Optional.of(TaskGroupState.RecoverableFailureCause.INPUT_READ_FAILURE));
          LOG.warn("{} Execution Failed (Recoverable)! Exception: {}",
              new Object[]{taskGroup.getTaskGroupId(), ex.toString()});
        } catch (final PartitionWriteException ex2) {
          taskGroupStateManager.onTaskGroupStateChanged(TaskGroupState.State.FAILED_RECOVERABLE,
              Optional.empty(),
              Optional.of(TaskGroupState.RecoverableFailureCause.OUTPUT_WRITE_FAILURE));
          LOG.warn("{} Execution Failed (Recoverable)! Exception: {}",
              new Object[]{taskGroup.getTaskGroupId(), ex2.toString()});
        } catch (final Exception e) {
          taskGroupStateManager.onTaskGroupStateChanged(TaskGroupState.State.FAILED_UNRECOVERABLE,
              Optional.empty(), Optional.empty());
          throw new RuntimeException(e);
        }
      });
    }
  }

  private boolean isLocalReadersEmpty(final Task task) {
    boolean allLocalReadersEmpty = true;

    for (OutputCollectorImpl localReader : taskIdToLocalReaderMap.get(task.getId())) {
      LOG.info("log: isLocalReadersEmpty: {} {} remaining {}", taskGroup.getTaskGroupId(),
          task.getRuntimeVertexId(), localReader.getQueueElements().toString());
      if (!localReader.isEmpty()) {
        allLocalReadersEmpty = false;
        break;
      }
    }

    return allLocalReadersEmpty;
  }

  public boolean isReadingFromBoundedSourceTaskComplete() {
    return hasBoundedSourceTask && !taskTypeToTaskIdMap.containsValue(BoundedSourceTask.class.getSimpleName());
  }

  public boolean isReadingFromAnotherStageComplete() {
    return sourceParallelism.get() == numOfInputStream.get();
  }

  public void checkTaskCompletion(final Task task) {
    boolean isComplete;

    if (task instanceof BoundedSourceTask) {
      isComplete = true;
    } else if (hasInputReader(task)) {
      isComplete = isReadingFromAnotherStageComplete();
    } else {
      // This task reads intra-stage data
      if (hasBoundedSourceTask) {
        // And its stage reads input from BoundedSourceTask.
        isComplete = isReadingFromBoundedSourceTaskComplete() && isLocalReadersEmpty(task);
      } else {
        // If else, it's parent task is another OperatorTask with LocalWriters.
        isComplete = isReadingFromAnotherStageComplete() && isLocalReadersEmpty(task);
      }
    }

    if (isComplete) {
      LOG.info("log: {} {} {} {} Complete!", taskGroup.getTaskGroupId(),
          task.getId(), task.getRuntimeVertexId(), task.getClass().getSimpleName());
      taskTypeToTaskIdMap.remove(task.getId());
    }
  }

  public boolean isTaskGroupComplete() {
    LOG.info("log: {} Complete!", taskGroup.getTaskGroupId());
    return taskTypeToTaskIdMap.isEmpty();
  }

  /**
   * Processes a BoundedSourceTask.
   * Reads input data from the bounded source at once.
   *
   * @param boundedSourceTask to execute.
   * @throws Exception RuntimeException.
   */
  private void launchBoundedSourceTask(final BoundedSourceTask boundedSourceTask) throws Exception {
    if (taskTypeToTaskIdMap.containsKey(boundedSourceTask.getId())) {
      LOG.info("log: Starting BoundedSourceTask! {} {} {}", taskGroup.getTaskGroupId(),
          boundedSourceTask.getId(), boundedSourceTask.getRuntimeVertexId());

      hasBoundedSourceTask = true;
      final Reader reader = boundedSourceTask.getReader();
      final Iterable readData = reader.read();

      // For inter-stage data, we need to write them to OutputWriters.
      // For intra-stage data, we need to emit data to OutputCollectorImpl.
      if (hasOutputWriter(boundedSourceTask)) {
        taskIdToOutputWriterMap.get(boundedSourceTask.getId()).forEach(outputWriter -> {
          outputWriter.write(readData);
          outputWriter.close();
        });
      } else {
        OutputCollectorImpl outputCollector = taskIdToLocalWriterMap.get(boundedSourceTask.getId());
        try {
          readData.forEach(data -> {
            outputCollector.emit(data);
            LOG.info("log: {} {} {} Adding {} to LocalWriter", taskGroup.getTaskGroupId(),
                boundedSourceTask.getId(), boundedSourceTask.getRuntimeVertexId(), data);
          });
        } catch (final Exception e) {
          throw new RuntimeException();
        }
      }

      checkTaskCompletion(boundedSourceTask);
    }
  }

  /**
   * Processes an OperatorTask.
   *
   * @param operatorTask to execute
   */
  private void launchOperatorTask(final OperatorTask operatorTask) {

    final Map<Transform, Object> sideInputMap = new HashMap<>();

    LOG.info("log: Start of launchOperatorTask {} {} {}", taskGroup.getTaskGroupId(),
        operatorTask.getId(), operatorTask.getRuntimeVertexId());
    LOG.info("log: sideInputReaders {} {}", taskGroup.getTaskGroupId(),
        taskIdToInputReaderMap.get(operatorTask.getId()));  //.stream().filter(InputReader::isSideInputReader));
    // Check for side inputs
    if (hasInputReader(operatorTask)) {
      taskIdToInputReaderMap.get(operatorTask.getId()).stream().filter(InputReader::isSideInputReader)
          .forEach(inputReader -> {
            try {
              final Object sideInput = inputReader.getSideInput().get();
              final RuntimeEdge inEdge = inputReader.getRuntimeEdge();
              final Transform srcTransform;
              if (inEdge instanceof PhysicalStageEdge) {
                srcTransform = ((OperatorVertex) ((PhysicalStageEdge) inEdge).getSrcVertex())
                    .getTransform();
              } else {
                srcTransform = ((OperatorTask) inEdge.getSrc()).getTransform();
              }
              sideInputMap.put(srcTransform, sideInput);
            } catch (final InterruptedException | ExecutionException e) {
              throw new PartitionFetchException(e);
            }
          });
    }

    final Transform.Context transformContext = new ContextImpl(sideInputMap);
    final Transform transform = operatorTask.getTransform();
    final OutputCollectorImpl outputCollector = taskIdToLocalWriterMap.get(operatorTask.getId());
    transform.prepare(transformContext, outputCollector);

    // Check for non-side inputs.
    final BlockingQueue<Object> dataQueue = new LinkedBlockingQueue<>();
    if (hasInputReader(operatorTask)) {
      // If this task accepts inter-stage data, read them from InputReader.
      // Inter-stage data is assumed to be sent element-wise.
      taskIdToInputReaderMap.get(operatorTask.getId()).stream().filter(inputReader -> !inputReader.isSideInputReader())
          .forEach(inputReader -> {
            // For inter-stage data, read them as Iterable.
            final List<CompletableFuture<Iterable>> futures = inputReader.read();
            // Add consumers which will push the data to the data queue when it ready to the futures.
            futures.forEach(compFuture -> compFuture.whenComplete((data, exception) -> {
              if (exception != null) {
                throw new PartitionFetchException(exception);
              }
              LOG.info("log: Reading from InputReader {} {} {}, data {}",
                  taskGroup.getTaskGroupId(), operatorTask.getId(), operatorTask.getRuntimeVertexId(),
                  data);

              dataQueue.add(data);
            }));
          });

      // Count the number of closed PartitionInputStream.
      // If this count is the same with source parallelism,
      // we can conclude that we received all of current TaskGroup's input.

      // Will first test for the MR, whose src parallelism is 1
      if (partitionManagerWorker.isInputStreamClosed()) {
        numOfInputStream.getAndIncrement();
        LOG.info("log: pmw inputstream closed {} {} {}", taskGroup.getTaskGroupId(), operatorTask.getId(),
            operatorTask.getRuntimeVertexId());
      }
    } else {
      // If else, this task accepts intra-stage data.
      // Intra-stage data are removed from parent Task's OutputCollectors, element-wise.
      taskIdToLocalReaderMap.get(operatorTask.getId())
          .forEach(localReader ->
          {
            if (!localReader.isEmpty()) {
              final Object output = localReader.remove();
              LOG.info("log: {} {}: Reading from LocalReader. output {}", taskGroup.getTaskGroupId(),
                  operatorTask.getId(), output.toString());
              dataQueue.add(output);
            }
          });
    }
    // Consumes the received element from incoming edges.
    IntStream.range(0, sourceParallelism.get()).forEach(srcTaskNum -> {
      try {
        Object data = dataQueue.take();
        LOG.info("log: {} {}: consume data {}", taskGroup.getTaskGroupId(),
            operatorTask.getId(), data.toString());

        // Because the data queue is a blocking queue, we may need to wait some available data to be pushed.
        transform.onData(data);
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }

      // For inter-stage data, we need to write them to OutputWriters, elements are bundled as a List.
      // For intra-stage data, child Tasks will read them from OutputCollectorImpls; no write operation is needed.
      final List output = outputCollector.collectOutputList();
      LOG.info("log: {} {}: output to OutputWriter {}", taskGroup.getTaskGroupId(),
          operatorTask.getId(), output.toString());
      if (!output.isEmpty()) {
        if (hasOutputWriter(operatorTask)) {
          taskIdToOutputWriterMap.get(operatorTask.getId()).forEach(outputWriter -> outputWriter.write(output));
        }
      }
    });
    transform.close();

    // Check whether there is any output data from the transform and write the output of this task to the writer.
    // Here, too, we need to consider only inter-stage data and write them to OutputWriters.
    final List output = outputCollector.collectOutputList();
    LOG.info("log: {} {}: output to OutputWriter {}", taskGroup.getTaskGroupId(),
        operatorTask.getId(), output.toString());

    if (!output.isEmpty()) {
      if (hasOutputWriter(operatorTask)) {
        taskIdToOutputWriterMap.get(operatorTask.getId()).forEach(outputWriter -> {
          outputWriter.write(output);
          outputWriter.close();
        });
      }
    }

    checkTaskCompletion(operatorTask);
  }

  /**
   * Pass on the data to the following tasks.
   *
   * @param task the task to carry on the data.
   */
  private void launchMetricCollectionBarrierTask(final MetricCollectionBarrierTask task) {
    final BlockingQueue<Iterable> dataQueue = new LinkedBlockingQueue<>();
    taskIdToInputReaderMap.get(task.getId()).stream().filter(inputReader -> !inputReader.isSideInputReader())
        .forEach(inputReader -> {
          inputReader.read().forEach(compFuture -> compFuture.thenAccept(dataQueue::add));
        });

    final List data = new ArrayList<>();
    IntStream.range(0, sourceParallelism.get()).forEach(srcTaskNum -> {
      try {
        final Iterable availableData = dataQueue.take();
        availableData.forEach(data::add);
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }
    });

    // For inter-stage data, we need to write them to OutputWriters.
    // For intra-stage data, we need to emit data to OutputCollectorImpl.
    if (!data.isEmpty()) {
      if (hasOutputWriter(task)) {
        taskIdToOutputWriterMap.get(task.getId()).forEach(outputWriter -> {
          outputWriter.write(data);
          outputWriter.close();
        });
      } else {
        OutputCollectorImpl outputCollector = taskIdToLocalWriterMap.get(task.getId());
        data.forEach(element -> {
          outputCollector.emit(element);
        });
      }
    }

    checkTaskCompletion(task);
  }
}
