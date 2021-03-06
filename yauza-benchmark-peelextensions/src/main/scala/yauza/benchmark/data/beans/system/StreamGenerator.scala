package yauza.benchmark.data.beans.system

import java.io.PrintWriter
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}

import com.samskivert.mustache.Mustache
import org.peelframework.core.beans.experiment.Experiment.Run
import org.peelframework.core.beans.system.Lifespan.Lifespan
import org.peelframework.core.beans.system.System
import org.peelframework.core.config.{Model, SystemConfig}
import org.peelframework.core.util.shell
import resource._
import yauza.benchmark.ResultsCollector

import scala.collection.JavaConverters._
import scala.collection.Seq
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/** Wrapper class for a stream generator.
  *
  * Implements stream generator runner as a Peel `System` and provides setup and teardown methods.
  *
  * @param version Version of the system (e.g. "7.1")
  * @param configKey The system configuration resides under `system.\${configKey}`
  * @param lifespan `Lifespan` of the system
  * @param dependencies Set of dependencies that this system needs
  * @param mc The moustache compiler to compile the templates that are used to generate property files for the system
  */
class StreamGenerator(
             version: String,
             configKey: String,
             lifespan: Lifespan,
             dependencies: Set[System] = Set(),
             mc: Mustache.Compiler,
             datagenName: String
           ) extends System("stream", version, configKey, lifespan, dependencies, mc) {

  def hosts: Set[String] = config.getStringList(s"system.$configKey.config.hosts").asScala.toSet

  var processes = Set.empty[ProcessDescriptor]

  // ---------------------------------------------------
  // System.
  // ---------------------------------------------------

  override def configuration() = SystemConfig(config, {
    val conf = config.getString(s"system.$configKey.path.config")
    List(
      SystemConfig.Entry[Model.Yaml]( s"system.$configKey.config",
        s"$conf/benchmark.properties", templatePath("config/benchmark.properties"), mc)
    )
  })


  override protected def start(): Unit = {
    val pidFle = Paths.get(config.getString(s"system.$configKey.path.pids"))

    // ensure that the PID file is writable
    // otherwise, create an empty file or throw an error if this is not possible
    if (!Files.exists(pidFle)) {
      Files.createDirectories(pidFle.getParent)
      shell.touch(pidFle.toString)
    }

    // ensure that the PID file is empty
    processes = for {
      line <- scala.io.Source.fromFile(pidFle.toString).getLines().toSet[String]
      desc <- ProcessDescriptor.unapply(line)
    } yield desc

    if (processes.nonEmpty) throw new RuntimeException(Seq(
      "It appears that some streamgenerator processes are still running on the following machines:",
      processes.map(desc => s"  - ${desc.host} with pid ${desc.pid}").mkString("\n"),
      "Please stop them first (e.g. using the `sys:teardown` command).").mkString("\n"))
  }

  override protected def stop(): Unit =
    stopProcesses(None)

  override def isRunning: Boolean =
    processes.size == hosts.size

  override def beforeRun(run: Run[System]): Unit = {
    kafkaTopicsInit
    startProcesses(run)
  }

  override def afterRun(run: Run[System]): Unit =
    stopProcesses(Some(run))

  // ---------------------------------------------------
  // Helper methods.
  // ---------------------------------------------------

  private def startProcesses(run: Run[System]): Unit = {
    val user = config.getString(s"system.$configKey.user")
    val datagen = config.getString("app.path.datagens") + "/" + datagenName
    val conf = config.getString(s"system.$configKey.path.config")

    logger.info("Starting stream generator processes on all hosts")
    val futureProcessDescriptors = Future.traverse(hosts)(host => Future {
      val cmd = Seq("java",
        "-jar", datagen,
        "--mode", "inmemory",
        "--config", s"${conf}/benchmark.properties"
      ).mkString(" ")

      val pid = (shell !! s""" ssh $host 'nohup $cmd >/dev/null 2>/dev/null & echo $$!' """).trim
      logger.info(s"stream generator started on host '$host' with PID $pid")

      ProcessDescriptor(host, pid.toInt)
    })
    setProcesses(Await.result(futureProcessDescriptors, Math.max(30, 5 * hosts.size).seconds))
  }

  private def stopProcesses(runOpt: Option[Run[System]]): Unit = {
    val user = config.getString(s"system.$configKey.user")

    val pidFle = Paths.get(config.getString(s"system.$configKey.path.pids"))

    if (Files.exists(pidFle) && processes.isEmpty)
      processes = for {
        line <- scala.io.Source.fromFile(pidFle.toString).getLines().toSet[String]
        desc <- ProcessDescriptor.unapply(line)
      } yield desc

    logger.info("Stopping stream generator processes on all hosts")
    val futureCopyContents = Future.traverse(processes)(desc => {
      for {
        killProcess <- Future {
          shell ! s"ssh ${desc.host} kill ${desc.pid}"
          logger.info(s"stream generator with PID ${desc.pid} stopped on host '${desc.host}'")
          desc
        }
      } yield killProcess
    })
    setProcesses(processes diff Await.result(futureCopyContents, Math.max(30, 5 * hosts.size).seconds))

    var resultPath:String = config.getString("app.path.results")

    val filename = for (run <- runOpt) yield {
      "results-" + run.home.substring(run.home.lastIndexOf('/') + 1) + ".json"
    }

    // collect data from Kafka
    ResultsCollector.fetchResults(
      config.getString(s"system.$configKey.path.config"),
      config.getString("app.path.results"),
      filename.getOrElse("result.json"),
      config.getString(s"system.$configKey.config.bootstrap.servers"),
      config.getString(s"system.$configKey.config.zookeeper.connect")
    )
  }

  private def setProcesses(processes: Set[ProcessDescriptor]) = {
    val pidFle = Paths.get(config.getString(s"system.$configKey.path.pids"))

    import StandardCharsets.UTF_8
    import StandardOpenOption.{CREATE, WRITE, TRUNCATE_EXISTING}

    for {
      buf <- managed(Files.newBufferedWriter(pidFle, UTF_8, CREATE, WRITE, TRUNCATE_EXISTING))
      out <- managed(new PrintWriter(buf))
    } for (desc <- processes) {
      out.write(s"$desc\n")
    }

    this.processes = processes
  }

  private def kafkaTopicsInit():Unit = {
    val topic = config.getString(s"system.$configKey.config.kafka.topic.input")
    val partitions = config.getString(s"system.$configKey.config.kafka.partition").toInt
    val outputTopics: Seq[String] = config.getStringList(s"system.$configKey.config.kafka.topic.outputs").asScala.toSeq

//    deleteTopic(topic)
    createTopic(topic, partitions)

//    outputTopics.foreach(deleteTopic)
//    Thread.sleep(5000)
  }

  def createTopic(inputTopic: String, partitions: Int = 1): Unit = {
    val dirName = config.getString(s"system.kafka.path.home")
    val zk_connections = config.getString(s"system.kafka.config.server.zookeeper.connect")

    if (!existsTopic(inputTopic)) {
      shell !! s"""$dirName/bin/kafka-topics.sh --create --zookeeper $zk_connections --replication-factor 1 --partitions $partitions --topic $inputTopic"""
    }
  }

  def deleteTopic(inputTopic: String): Unit = {
    val dirName = config.getString(s"system.kafka.path.home")
    val zk_connections = config.getString(s"system.kafka.config.server.zookeeper.connect")

    if (existsTopic(inputTopic)) {
      shell !! s"""$dirName/bin/kafka-topics.sh --delete --zookeeper $zk_connections --topic $inputTopic"""
    }
  }

  private def existsTopic(inputTopic: String): Boolean = {
    val dirName = config.getString(s"system.kafka.path.home")
    val zk_connections = config.getString(s"system.kafka.config.server.zookeeper.connect")

    val count: Int = (shell !! s"""$dirName/bin/kafka-topics.sh --describe --zookeeper $zk_connections --topic $inputTopic 2>/dev/null""")
      .split("\n").find(str => str.contains(inputTopic)).size

    return count > 0
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
