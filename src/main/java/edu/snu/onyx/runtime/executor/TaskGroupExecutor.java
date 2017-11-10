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

import edu.snu.onyx.common.Pair;
import edu.snu.onyx.compiler.ir.*;
//import edu.snu.onyx.compiler.ir.executionproperty.ExecutionProperty;
//import edu.snu.onyx.runtime.common.RuntimeIdGenerator;
import edu.snu.onyx.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.runtime.common.plan.RuntimeEdge;
import edu.snu.onyx.runtime.common.plan.physical.*;
import edu.snu.onyx.runtime.common.state.TaskGroupState;
import edu.snu.onyx.runtime.common.state.TaskState;
import edu.snu.onyx.runtime.exception.PartitionFetchException;
import edu.snu.onyx.runtime.exception.PartitionWriteException;
import edu.snu.onyx.runtime.executor.data.PartitionManagerWorker;
import edu.snu.onyx.runtime.executor.datatransfer.DataTransferFactory;
import edu.snu.onyx.runtime.executor.datatransfer.InputReader;
import edu.snu.onyx.runtime.executor.datatransfer.OutputWriter;
import edu.snu.onyx.runtime.master.irimpl.ContextImpl;
import edu.snu.onyx.runtime.master.irimpl.OutputCollectorImpl;
//import edu.snu.onyx.common.dag.DAG;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.jvm.hotspot.jdi.ArrayReferenceImpl;

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
  private final AtomicInteger sourceParallelism;
  // For intra-TaskGroup element-wise data transfer.
  // Inter-Task data are transferred via OutputCollector of each Task.
  private final List<Pair<String, OutputCollectorImpl>> taskIdToOutputCollectorList;

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

    this.sourceParallelism = new AtomicInteger(0);
    this.taskIdToOutputCollectorList = new ArrayList<>();

    this.taskIdToInputReaderMap = new HashMap<>();
    this.taskIdToOutputWriterMap = new HashMap<>();

    this.isExecutionRequested = false;

    initializeDataTransfer();
  }

  /**
   * Initializes readers and writers depending on the execution properties.
   * Reader and writer exist per TaskGroup, so we consider only cross-stage edges.
   */
  private void initializeDataTransfer() {
    final AtomicInteger taskIdx = new AtomicInteger(0);

    taskGroup.getTaskDAG().topologicalDo((task -> {
      final Set<PhysicalStageEdge> inEdgesFromOtherStages = getInEdgesFromOtherStages(task);
      final Set<PhysicalStageEdge> outEdgesToOtherStages = getOutEdgesToOtherStages(task);

      inEdgesFromOtherStages.forEach(physicalStageEdge -> {
        final InputReader inputReader = channelFactory.createReader(
            task, physicalStageEdge.getSrcVertex(), physicalStageEdge);
        addInputReader(task, inputReader);
        sourceParallelism.getAndAdd(physicalStageEdge.getSrcVertex().getProperty(ExecutionProperty.Key.Parallelism));
      });

      outEdgesToOtherStages.forEach(physicalStageEdge -> {
        final OutputWriter outputWriter = channelFactory.createWriter(
            task, physicalStageEdge.getDstVertex(), physicalStageEdge);
        addOutputWriter(task, outputWriter);
      });

      addOutputCollector(task, taskIdx.getAndIncrement());
    }));
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

  private void addOutputCollector(final Task task, final int taskIdx) {
    taskIdToOutputCollectorList.add(Pair.of(task.getId(), new OutputCollectorImpl()));
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
    LOG.info("{} Execution Started!", taskGroup.getTaskGroupId());
    if (isExecutionRequested) {
      throw new RuntimeException("TaskGroup {" + taskGroup.getTaskGroupId() + "} execution called again!");
    } else {
      isExecutionRequested = true;
    }

    taskGroupStateManager.onTaskGroupStateChanged(TaskGroupState.State.EXECUTING, Optional.empty(), Optional.empty());

    taskGroup.getTaskDAG().topologicalDo(task -> {
      taskGroupStateManager.onTaskStateChanged(task.getId(), TaskState.State.EXECUTING, Optional.empty());
      try {
        if (task instanceof BoundedSourceTask) {
          launchBoundedSourceTask((BoundedSourceTask) task);
          taskGroupStateManager.onTaskStateChanged(task.getId(), TaskState.State.COMPLETE, Optional.empty());
          LOG.info("{} Execution Complete!", taskGroup.getTaskGroupId());
        } else if (task instanceof OperatorTask) {
          launchOperatorTask((OperatorTask) task);
          taskGroupStateManager.onTaskStateChanged(task.getId(), TaskState.State.COMPLETE, Optional.empty());
          LOG.info("{} Execution Complete!", taskGroup.getTaskGroupId());
        } else if (task instanceof MetricCollectionBarrierTask) {
          launchMetricCollectionBarrierTask((MetricCollectionBarrierTask) task);
          taskGroupStateManager.onTaskStateChanged(task.getId(), TaskState.State.ON_HOLD, Optional.empty());
          LOG.info("{} Execution Complete!", taskGroup.getTaskGroupId());
        } else {
          throw new UnsupportedOperationException(task.toString());
        }
      } catch (final PartitionFetchException ex) {
        taskGroupStateManager.onTaskStateChanged(task.getId(), TaskState.State.FAILED_RECOVERABLE,
            Optional.of(TaskGroupState.RecoverableFailureCause.INPUT_READ_FAILURE));
        LOG.warn("{} Execution Failed (Recoverable)! Exception: {}",
            new Object[] {taskGroup.getTaskGroupId(), ex.toString()});
      } catch (final PartitionWriteException ex2) {
        taskGroupStateManager.onTaskStateChanged(task.getId(), TaskState.State.FAILED_RECOVERABLE,
            Optional.of(TaskGroupState.RecoverableFailureCause.OUTPUT_WRITE_FAILURE));
        LOG.warn("{} Execution Failed (Recoverable)! Exception: {}",
            new Object[] {taskGroup.getTaskGroupId(), ex2.toString()});
      } catch (final Exception e) {
        taskGroupStateManager.onTaskStateChanged(task.getId(), TaskState.State.FAILED_UNRECOVERABLE, Optional.empty());
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Processes a BoundedSourceTask.
   * Reads input data from the bounded source at once.
   * @param boundedSourceTask to execute
   */
  private void launchBoundedSourceTask(final BoundedSourceTask boundedSourceTask) throws Exception {
    final Reader reader = boundedSourceTask.getReader();
    final Iterable readData = reader.read();

    taskIdToOutputWriterMap.get(boundedSourceTask.getId()).forEach(outputWriter -> {
      outputWriter.write(readData);
      outputWriter.close();
    });
  }

  /**
   * Processes an OperatorTask.
   * @param operatorTask to execute
   */
  private void launchOperatorTask(final OperatorTask operatorTask) {
    final Map<Transform, Object> sideInputMap = new HashMap<>();

    // Check for side inputs
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

    final Transform.Context transformContext = new ContextImpl(sideInputMap);
    final Transform transform = operatorTask.getTransform();
    final OutputCollectorImpl outputCollector = taskIdToOutputCollectorMap.get(operatorTask.getId());
    transform.prepare(transformContext, outputCollector);

    // Check for non-side inputs.
    final BlockingQueue<Object> dataQueue = new LinkedBlockingQueue<>();
    if (hasInputReader(operatorTask)) {
      // If this task accepts inter-stage data, read them from InputReader.
      taskIdToInputReaderMap.get(operatorTask.getId()).stream().filter(inputReader -> !inputReader.isSideInputReader())
          .forEach(inputReader -> {
            final List<CompletableFuture<Object>> futures = inputReader.readElement();
            // Add consumers which will push the data to the data queue when it ready to the futures.
            futures.forEach(compFuture -> compFuture.whenComplete((data, exception) -> {
              if (exception != null) {
                throw new PartitionFetchException(exception);
              }
              dataQueue.add(data);
            }));
          });
    } else {
      // If else, this task accepts intra-stage data.
      // Read them from previous Task's OutputCollector.

      // Here, this task should know who the 'prev Task' is.
      // For this, taskIdToOutputCollector should be a list that contains topo-sorted Tasks.
      // Thus, initialize taskIdToOutputCollector from initializeDataTransfer().
    }

    // Consumes all of the partitions from incoming edges.
    IntStream.range(0, sourceParallelism.get()).forEach(srcTaskNum -> {
      try {
        // Because the data queue is a blocking queue, we may need to wait some available data to be pushed.
        transform.onData(dataQueue.take());
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }

      if (hasOutputWriter(operatorTask)) {
        // Check whether there is any output data from the transform and write the output of this task to the writer.
        final List output = outputCollector.collectOutputList();
        if (!output.isEmpty()) {
          taskIdToOutputWriterMap.get(operatorTask.getId()).forEach(outputWriter -> outputWriter.write(output));
        } // If else, this is a sink task.
      }
    });
    transform.close();

    // Check whether there is any output data from the transform and write the output of this task to the writer.
    final List output = outputCollector.collectOutputList();
    if (hasOutputWriter(operatorTask)) {
      taskIdToOutputWriterMap.get(operatorTask.getId()).forEach(outputWriter -> {
        if (!output.isEmpty()) {
          outputWriter.write(output);
        }
        outputWriter.close();
      });
    } else {
      LOG.info("This is a sink task: {}", operatorTask.getId());
    }
  }

  /**
   * Pass on the data to the following tasks.
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
        throw new PartitionFetchException(e);
      }
    });
    taskIdToOutputWriterMap.get(task.getId()).forEach(outputWriter -> {
      outputWriter.write(data);
      outputWriter.close();
    });
  }
}
