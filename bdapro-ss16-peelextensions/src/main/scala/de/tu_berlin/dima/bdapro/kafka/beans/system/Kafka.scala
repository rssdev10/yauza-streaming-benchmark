package de.tu_berlin.dima.bdapro.kafka.beans.system

import java.io.PrintWriter
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.regex.Pattern

import com.samskivert.mustache.Mustache
import org.peelframework.core.beans.experiment.Experiment.Run
import org.peelframework.core.beans.system.Lifespan.Lifespan
import org.peelframework.core.beans.system.{DistributedLogCollection, System}
import org.peelframework.core.config.{Model, SystemConfig}
import org.peelframework.core.util.shell
import resource._

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

  override def hosts: Seq[String]  = config.getStringList(s"system.$configKey.config.hosts").asScala

  var processes: Seq[ProcessDescriptor] = Seq.empty[ProcessDescriptor]

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
      SystemConfig.Entry[Model.Yaml](          // render using the 'Model.Yaml' controller
        s"system.$configKey.config.yaml",      // use this config namespace as a 'model'
        s"$conf/kafka-conf.yaml",              // instantiate the config file
        templatePath("conf/kafka-conf.yaml"),  // use this template to render the file
        mc
      )
    ) // return a list of configuration files to render on setup
  })

  override protected def start(): Unit = {
    val kafkaPath = config.getString(s"system.$configKey.path.home")
    val logDir = config.getString(s"system.$configKey.path.log")

    logger.info("Starting kafka processes on all hosts")
    val futureProcessDescriptors: Future[Seq[ProcessDescriptor]] = Future.traverse(hosts)(host => Future {
      val cmd: String = Seq(
        s"${kafkaPath}/bin/kafka-server-start.sh",
        s"${kafkaPath}/config/server.properties"
      ).mkString(" ")

      val pid = (shell !! s""" ssh $host 'rm -f "$logDir"; nohup $cmd >/dev/null 2>/dev/null & echo $$!' """).trim
      logger.info(s"Kafka started on host '$host' with PID $pid")

      ProcessDescriptor(host, pid.toInt)
    })
    setProcesses(Await.result(futureProcessDescriptors, Math.max(30, 5 * hosts.size).seconds))
    createTopic()
  }

  override protected def stop(): Unit = {
  }

  private def stopProcesses(runOpt: Option[Run[System]]): Unit = {
    val user = config.getString(s"system.$configKey.user")

    val pidFle = Paths.get(config.getString(s"system.$configKey.path.pids"))

    logger.info("Stopping Kafka processes on all hosts")
    val futureKillProcess = Future.traverse(hosts)(host => Future {
      shell ! s"""
         |ssh $$host << SSHEND
         |  PID==`ps -aef | grep "[k]afka.Kafka" | awk '{print  $$2}' |  head -1`
         |
         |  if [[ "$$PID" -ne "" ]]; then
         |    kill "$$PID"
         |    sleep 3
         |  fi
         |
         |  PID==`ps -aef | grep "[k]afka.Kafka" | awk '{print  $$2}' |  head -1`
         |
         |  if [[ "$$PID" -ne "" ]]; then
         |    kill -9 "$$PID"
         |  fi
         |
         |  exit
         |SSHEND
         |""".stripMargin
      logger.info(s"Kafka stopped on host '${host}'")
    })
    Await.result(futureKillProcess, Math.max(30, 5 * hosts.size).seconds)
  }

  override def isRunning: Boolean =
    processes.size == hosts.size

  // ---------------------------------------------------
  // Helper methods.
  // ---------------------------------------------------

  private def setProcesses(processes: Seq[ProcessDescriptor]) = {
    val pidFle = Paths.get(config.getString(s"system.$configKey.path.pids"))

    import StandardCharsets.UTF_8
    import StandardOpenOption.{CREATE, TRUNCATE_EXISTING, WRITE}

    for {
      buf <- managed(Files.newBufferedWriter(pidFle, UTF_8, CREATE, WRITE, TRUNCATE_EXISTING))
      out <- managed(new PrintWriter(buf))
    } for (desc <- processes) {
      out.write(s"$desc\n")
    }

    this.processes = processes
  }

  private def createTopic():Unit = {
    val dirName = config.getString(s"system.$configKey.path.home")
    val zk_connections = config.getString(s"system.$configKey.zookeeper")
    val partitions: Int = config.getString(s"system.$configKey.partitions").toInt
    val inputTopic = config.getString(s"system.$configKey.inputTopic")

    val count = (shell !! s"""$dirName/bin/kafka-topics.sh --describe --zookeeper $zk_connections --topic $inputTopic 2>/dev/null""")
      .split("\n").find(str => str.contains(inputTopic)).size

    if (count == 0) {
      shell !! s"""$dirName/bin/kafka-topics.sh --create --zookeeper $zk_connections --replication-factor 1 --partitions $partitions --topic $inputTopic"""
    } else {
      println(s"Kafka topic $inputTopic already exists")
    }
  }
}

case class ProcessDescriptor(host: String, pid: Int)

object ProcessDescriptor {
  val ProcessDescriptorRegex = "ProcessDescriptor\\((.*),(\\d+)\\)".r

  def unapply(s: String): Option[ProcessDescriptor] = s match {
    case ProcessDescriptorRegex(host, pid) => Some(new ProcessDescriptor(host, pid.toInt))
    case _ => None
  }
}
