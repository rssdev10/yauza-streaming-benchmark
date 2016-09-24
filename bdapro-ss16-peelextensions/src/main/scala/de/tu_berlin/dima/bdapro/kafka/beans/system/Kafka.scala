package de.tu_berlin.dima.bdapro.kafka.beans.system

import java.util.regex.Pattern

import com.samskivert.mustache.Mustache
import org.peelframework.core.beans.system.Lifespan.Lifespan
import org.peelframework.core.beans.system.{DistributedLogCollection, System}
import org.peelframework.core.config.{Model, SystemConfig}
import org.peelframework.core.util.shell

import scala.collection.JavaConverters._
import scala.collection.Seq
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
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

  override def hosts: Seq[String] = config.getStringList(s"system.$configKey.config.hosts").asScala

  override protected def logFilePatterns(): Seq[Regex] = {
    hosts.map(Pattern.quote).flatMap(node => Set(
      s"server.log\\.*".r,
      s"state-change.log\\.*".r,
      s"controller.log\\.*".r))
  }
  // ---------------------------------------------------
  // System.
  // ---------------------------------------------------

  override def configuration() = SystemConfig(config, {
    val conf = config.getString(s"system.$configKey.path.config")
    List(
      SystemConfig.Entry[Model.Yaml](             // render using the 'Model.Yaml' controller
        s"system.$configKey.server.properties",   // use this config namespace as a 'model'
        s"$conf/server.properties",               // instantiate the config file
        templatePath("config/server.properties"), // use this template to render the file
        mc
      )
    ) // return a list of configuration files to render on setup
  })

  override protected def start(): Unit = {
    val kafkaPath = config.getString(s"system.$configKey.path.home")
    val kafkaTmp = config.getString(s"system.$configKey.path.kafka.tmp")
    val logDir = config.getString(s"system.$configKey.path.log")
    val timeout: Int = config.getString(s"system.$configKey.startup.timeout").toInt

    logger.info("Starting kafka processes on all hosts")
    val futureProcessDescriptors: Future[Seq[(String, Int)]] = Future.traverse(hosts)(host => Future {
      val cmd: String = Seq(
        s"rm -rf ${kafkaTmp};",                  // TODO: make cleaing of Kafka dir more safe for incorrent dir
        s"export LOG_DIR=${logDir};",
        s"${kafkaPath}/bin/kafka-server-start.sh",
        s"${kafkaPath}/config/server.properties" // TODO: specify properties file unique for each node
      ).mkString(" ")

      shell ! s""" ssh $host 'nohup $cmd >/dev/null 2>/dev/null & echo $$!' """

      Thread.sleep(timeout * 1000)

      val pid = sshFindKafkaPid(host)

      (host, pid)
    })
    Await.result(futureProcessDescriptors, Math.max(30, 5 * hosts.size).seconds).foreach { case (host, pid) =>
      if (pid > 0)
        logger.info(s"Kafka started on host '$host' with PID $pid")
      else
        logger.error(s"Kafka didn't start on host '$host'")
    }

    createTopic("test-topic", 2) // TODO: remove after testing
  }

  override protected def stop(): Unit = {
    val user = config.getString(s"system.$configKey.user")

    logger.info("Stopping Kafka processes on all hosts")
    val futureKillProcess = Future.traverse(hosts)(host => Future {
      var pid = sshFindKafkaPid(host)
      if (pid > 0) {
        shell ! s"ssh $host kill $pid"

        Thread sleep(5000)

        pid = sshFindKafkaPid(host)
        if (pid > 0) {
          shell ! s"ssh $host kill -9 $pid"
        }
      }
      logger.info(s"Kafka stopped on host '${host}'")
    })
    Await.result(futureKillProcess, Math.max(30, 5 * hosts.size).seconds)
  }

  override def isRunning: Boolean = {
    val futureProcessDescriptors: Future[Seq[Int]] = Future.traverse(hosts)(host => Future {
      sshFindKafkaPid(host)
    })
    Await.result(futureProcessDescriptors, Math.max(30, 5 * hosts.size).seconds).exists(_ > 0)
  }

    // ---------------------------------------------------
  // Helper methods.
  // ---------------------------------------------------

  def sshFindPidByTemplate(host:String, template: String):Int = {
    val pid = shell.!!(s"""ssh $host "ps -aef | grep "$template" | grep -vE 'grep' | awk '{print \\$$2}' | head -1"""").trim
    try {
      pid.toInt
    } catch {
      case e: NumberFormatException => 0
    }
  }

  private def sshFindKafkaPid(host:String):Int = sshFindPidByTemplate(host, "[k]afka.Kafka")

  def createTopic(inputTopic:String, partitions:Int = 1):Unit = {
    val dirName = config.getString(s"system.$configKey.path.home")
    val zk_connections = config.getString(s"system.$configKey.server.properties.zookeeper.connect")

    val count = (shell !! s"""$dirName/bin/kafka-topics.sh --describe --zookeeper $zk_connections --topic $inputTopic 2>/dev/null""")
      .split("\n").find(str => str.contains(inputTopic)).size

    if (count == 0) {
      shell !! s"""$dirName/bin/kafka-topics.sh --create --zookeeper $zk_connections --replication-factor 1 --partitions $partitions --topic $inputTopic"""
    } else {
      logger.info(s"Kafka topic $inputTopic already exists")
    }
  }
}
