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
package edu.snu.vortex.compiler.frontend.spark

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.collection.Map
import scala.reflect.ClassTag

/**
  * Created by wonook on 07/08/2017.
  */
class SparkContext(config: SparkConf) {
  private val allowMultipleContexts: Boolean =
    config.getBoolean("spark.driver.allowMultipleContexts", false)

  private[spark] val stopped: AtomicBoolean = new AtomicBoolean(false)

  def this() = this(new SparkConf())
  def this(master: String, appName: String, conf: SparkConf) =
    this(SparkContext.updatedConf(conf, master, appName))
  def this(
            master: String,
            appName: String,
            sparkHome: String = null,
            jars: Seq[String] = Nil,
            environment: Map[String, String] = Map()) = {
    this(SparkContext.updatedConf(new SparkConf(), master, appName, sparkHome, jars, environment))
  }

  private[spark] def this(master: String, appName: String) =
    this(master, appName, null, Nil, Map())
  private[spark] def this(master: String, appName: String, sparkHome: String) =
    this(master, appName, sparkHome, Nil, Map())
  private[spark] def this(master: String, appName: String, sparkHome: String, jars: Seq[String]) =
    this(master, appName, sparkHome, jars, Map())

  private var _conf: SparkConf = _
  private var _applicationId: String = _
  private var _applicationAttemptId: Option[String] = None
  private var _jars: Seq[String] = _
  private var _files: Seq[String] = _

  private[spark] def conf: SparkConf = _conf

  def getConf: SparkConf = conf.clone()

  def jars: Seq[String] = _jars
  def files: Seq[String] = _files
  def master: String = _conf.get("spark.master")
  def deployMode: String = _conf.getOption("spark.submit.deployMode").getOrElse("client")
  def appName: String = _conf.get("spark.app.name")

  def isStopped: Boolean = stopped.get()

  def applicationId: String = _applicationId
  def applicationAttemptId: Option[String] = _applicationAttemptId


  def parallelize[T: ClassTag](seq: Seq[T],
                               numSlices: Int = defaultParallelism): RDD[T] = withScope {
    new ParallelCollectionRDD[T](this, seq, numSlices, Map[Int, Seq[String]]())
  }

  def stop(): Unit = {
    // Use the stopping variable to ensure no contention for the stop scenario.
    // Still track the stopped variable for use elsewhere in the code.
    if (!stopped.compareAndSet(false, true)) {
      return
    }
  }



  def runJob[T, U: ClassTag](rdd: RDD[T],
                             processPartition: Iterator[T] => U,
                             resultHandler: (Int, U) => Unit) {
    val processFunc = (context: TaskContext, iter: Iterator[T]) => processPartition(iter)
    runJob[T, U](rdd, processFunc, 0 until rdd.partitions.length, resultHandler)
  }

  private[spark] def clean[F <: AnyRef](f: F, checkSerializable: Boolean = true): F = {
    // INCOMPLETE
    if (f == null) {
      return f
    }
    f
  }

  SparkContext.setActiveContext(this, allowMultipleContexts)
}

object SparkContext {
  private val SPARK_CONTEXT_CONSTRUCTOR_LOCK = new Object()

  private val activeContext: AtomicReference[SparkContext] =
    new AtomicReference[SparkContext](null)

  private var contextBeingConstructed: Option[SparkContext] = None

  def getOrCreate(config: SparkConf): SparkContext = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      if (activeContext.get() == null) {
        setActiveContext(new SparkContext(config), allowMultipleContexts = false)
      }
      activeContext.get()
    }
  }

  def getOrCreate(): SparkContext = {
    getOrCreate(new SparkConf())
  }

  private[spark] def setActiveContext(sc: SparkContext,
                                      allowMultipleContexts: Boolean): Unit = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      contextBeingConstructed = None
      activeContext.set(sc)
    }
  }
}
