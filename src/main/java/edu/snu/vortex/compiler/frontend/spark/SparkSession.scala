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

import java.io.Closeable
import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable

class SparkSession private(@transient val sparkContext: SparkContext)
  extends Serializable with Closeable { self =>

  private[sql] def this(sc: SparkContext) {
    this(sc)
  }

  def stop(): Unit = {
    sparkContext.stop()
  }

  override def close(): Unit = stop()
}

object SparkSession {
  class Builder {
    private[this] val options = new mutable.HashMap[String, String]

    private[this] var userSuppliedContext: Option[SparkContext] = None

    private[spark] def sparkContext(sparkContext: SparkContext): Builder = synchronized {
      userSuppliedContext = Option(sparkContext)
      this
    }

    def appName(name: String): Builder = config("spark.app.name", name)

    def config(key: String, value: String): Builder = synchronized {
      options += key -> value
      this
    }
    def config(key: String, value: Long): Builder = synchronized {
      options += key -> value.toString
      this
    }
    def config(key: String, value: Double): Builder = synchronized {
      options += key -> value.toString
      this
    }
    def config(key: String, value: Boolean): Builder = synchronized {
      options += key -> value.toString
      this
    }
//    def config(conf: SparkConf): Builder = synchronized {
//      conf.getAll.foreach { case (k, v) => options += k -> v }
//      this
//    }

    def getOrCreate(): SparkSession = synchronized {
      var session = activeThreadSession.get()
      if ((session ne null) && !session.sparkContext.isStopped) {
        return session
      }

      SparkSession.synchronized {
        session = defaultSession.get()
        if ((session ne null) && !session.sparkContext.isStopped) {
          return session
        }

        // No active nor global default session. Create a new one.
        val sparkContext = userSuppliedContext.getOrElse {
          val randomAppName = java.util.UUID.randomUUID().toString
          //        val sparkConf = new SparkConf()
          //        options.foreach {
          //          case (k, v) => sparkConf.set(k, v)
          //        }
          //        if (!sparkConf.contains("spark.app.name")) {
          //          sparkConf.setAppName(randomAppName)
          //        }
          //        val sc = SparkContext.getOrCreate(sparkConf)
          //        options.foreach {
          //          case (k, v) => s  c.conf.set(k, v)
          //        }
          //        if (!sc.conf.contains("spark.app.name")) {
          //          sc.conf.setAppName(randomAppName)
          //        }
          //        sc
          new SparkContext
        }
        session = new SparkSession(sparkContext)
        defaultSession.set(session)
      }

      return session
    }
  }

  def builder(): Builder = new Builder

  private val activeThreadSession = new InheritableThreadLocal[SparkSession]

  private val defaultSession = new AtomicReference[SparkSession]
}