package de.tu_berlin.dima.bdapro.kafka.beans.system

import java.util.regex.Pattern

import com.samskivert.mustache.Mustache
import org.peelframework.core.beans.system.Lifespan.Lifespan
import org.peelframework.core.beans.system.{DistributedLogCollection, SetUpTimeoutException, System}
import org.peelframework.core.config.{Model, SystemConfig}
import org.peelframework.core.util.shell

import scala.collection.JavaConverters._
import scala.collection.Seq
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.util.matching.Regex

/** Wrapper class for Kafka.
  *
  * Implements Kafka as a Peel `System` and provides setup and teardown methods.
  *
  * @param version Version of the system (e.g. "7.1")
  * @param configKey The system configuration resides under `system.\${configKey}`
  * @param lifespan `Lifespan` of the system
  * @param dependencies Set of dependencies that this system needs
  * @param mc The moustache compiler to compile the templates that are used to generate property files for the system
  */
class Kafka(
             version      : String,
             configKey    : String,
             lifespan     : Lifespan,
             dependencies : Set[System] = Set(),
             mc           : Mustache.Compiler) extends System("kafka", version, configKey, lifespan, dependencies, mc)
  with DistributedLogCollection {

  // ---------------------------------------------------
  // LogCollection.
  // ---------------------------------------------------

  override def hosts: Seq[String] = ???

  override protected def logFilePatterns(): Seq[Regex] = ???

  // ---------------------------------------------------
  // System.
  // ---------------------------------------------------

  override def configuration() = SystemConfig(config, {
    val conf = config.getString(s"system.$configKey.path.config")
    List(
      SystemConfig.Entry[Model.Yaml](          // render using the 'Model.Yaml' controller
        s"system.$configKey.config.yaml",      // use this config namespace as a 'model'
        s"$conf/kafka-conf.yaml",              // instantiate the config file
        templatePath("conf/kafka-conf.yaml"),  // use this template to render the file
        mc
      )
    ) // return a list of configuration files to render on setup
  })

  override protected def start(): Unit = ???   // TODO: implement

  override protected def stop(): Unit = ???    // TODO: implement

  override def isRunning: Boolean = ???        // TODO: implement
}
