package yauza.benchmark.kafka.beans.system

import com.samskivert.mustache.Mustache
import org.peelframework.core.beans.system.Lifespan.Lifespan
import org.peelframework.core.beans.system.{FileSystem, System}
import org.peelframework.core.config.{Model, SystemConfig}
import org.peelframework.core.util.shell

import scala.collection.JavaConverters._
import scala.collection.Seq
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/** Wrapper class for Kafka.
 *
 * Implements Kafka as a Peel `System` and provides setup and teardown methods.
 *
 * @param version      Version of the system (e.g. "7.1")
 * @param configKey    The system configuration resides under `system.\${configKey}`
 * @param lifespan     `Lifespan` of the system
 * @param dependencies Set of dependencies that this system needs
 * @param mc           The moustache compiler to compile the templates that are used to generate property files for the system
 */
class Kafka(
  version: String,
  configKey: String,
  lifespan: Lifespan,
  dependencies: Set[System] = Set(),
  mc: Mustache.Compiler
) extends System("kafka", version, configKey, lifespan, dependencies, mc) with FileSystem  {

  def hosts: Seq[String] = config.getStringList(s"system.$configKey.config.hosts").asScala

  // ---------------------------------------------------
  // System.
  // ---------------------------------------------------

  override def configuration() = new SystemConfig(config, {
    val conf = config.getString(s"system.$configKey.path.config")
    List(
      SystemConfig.Entry[Model.Yaml](s"system.$configKey.config.server", s"$conf/server.properties", templatePath("config/server.properties"), mc)
    )
  }) with ReplicateServerConfigs

  protected def cleanupServerDirsCmd(host: String): String = {
    val user = config.getString(s"system.$configKey.user")
    val log = config.getString(s"system.$configKey.path.log")
    val binlogDirs = config.getString(s"system.$configKey.config.server.log.dirs").split(',')
    val initDirs = log +: binlogDirs // directories to be initialized on each host
    val cmd = initDirs.map(path => s"rm -Rf $path && mkdir -p $path").mkString(" && ")
    s""" ssh $user@$host "$cmd" """
  }

  override protected def start(): Unit = {
    val user = config.getString(s"system.$configKey.user")
    val home = config.getString(s"system.$configKey.path.home")
    val log = config.getString(s"system.$configKey.path.log")
    val startUpTimeout = config.getString(s"system.$configKey.startup.timeout").toInt

    val startServerCmd = (host: String) =>
      s"""
         |ssh -tt $user@$host << SSHEND
         |  export export LOG_DIR=$log
         |  $home/bin/kafka-server-start.sh -daemon $home/config/server-$host.properties >/dev/null 2>/dev/null &
         |  exit
         |SSHEND
         """.stripMargin.trim

    val checkServerHealthCmd = (host: String) =>
      s""" ssh $user@$host "ps ax | grep -i 'kafka\\.Kafka' | grep java | grep -v grep | awk '{print $$1}'" """

    logger.info("Starting kafka processes on all hosts")
    val futures = Future.traverse(hosts)(host => for {
      initServerDirs <- Future {
        logger.info(s"Initializing Kafka directories at $host")
        shell ! (cleanupServerDirsCmd(host), s"Unable to initialize Kafka directories at $host.")
      }
      startServer <- Future {
        logger.info(s"Starting Kafka server at $host")
        shell ! (startServerCmd(host), s"Unable to start Kafka server on host '$host'.")
      }
      checkServerHealth <- Future {
        Thread.sleep(startUpTimeout * 1000)
        logger.info(s"Checking health of Kafka server at $host")
        shell !! checkServerHealthCmd(host)
      }
    } yield checkServerHealth)

    val pids = Await.result(futures, Math.max(30, 5 * hosts.size).seconds)

    // mark the system as `up` if all results are non-empty PIDs
    isUp = pids.forall(_.trim.nonEmpty)
  }

  override protected def stop(): Unit = {
    val user = config.getString(s"system.$configKey.user")
    val home = config.getString(s"system.$configKey.path.home")

    val stopServerCmd = (host: String) =>
      s"""
         |ssh $user@$host $home/bin/kafka-server-stop.sh
         """.stripMargin.trim

    val checkServerHealthCmd = (host: String) =>
      s""" ssh $user@$host "ps ax | grep -i 'kafka\\.Kafka' | grep java | grep -v grep | awk '{print $$1}'" """

    logger.info("Stopping Kafka processes on all hosts")
    val futures = Future.traverse(hosts)(host => for {
      cleanupServerDirs <- Future {
        logger.info(s"Cleaning up Kafka directories at $host")
        shell ! (cleanupServerDirsCmd(host), s"Unable to cleanup Kafka directories at $host.")
      }
      stopServer <- Future {
        logger.info(s"Stopping Kafka server at $host")
        shell ! (stopServerCmd(host), s"Unable to stop Kafka server on host '$host'.")
      }
      checkServerHealth <- Future {
        shell !! checkServerHealthCmd(host)
      }
    } yield checkServerHealth)

    val pids = Await.result(futures, Math.max(30, 5 * hosts.size).seconds)

    // mark the system as `down` if all PID results are empty
    isUp = pids.forall(_.trim.isEmpty)
  }

  override def isRunning: Boolean = {
    val user = config.getString(s"system.$configKey.user")
    val checkServerHealthCmd = (host: String) =>
      s""" ssh $user@$host "ps ax | grep -i 'kafka\\.Kafka' | grep java | grep -v grep | awk '{print $$1}'" """

    val futures = Future.traverse(hosts)(host => for {
      checkServerHealth <- Future {
        shell !! checkServerHealthCmd(host)
      }
    } yield checkServerHealth)

    val pids = Await.result(futures, Math.max(30, 5 * hosts.size).seconds)

    // report the system as `running` if all PID results are non-empty
    pids.forall(_.trim.nonEmpty)
  }

  override def exists(path: String): Boolean = {
    val dirName = config.getString(s"system.$configKey.path.home")
    val zk_connections = config.getString(s"system.$configKey.config.server.zookeeper.connect")

    val count = (shell !! s"""$dirName/bin/kafka-topics.sh --describe --zookeeper $zk_connections --topic $path 2>/dev/null""")
      .split("\n").find(str => str.contains(path)).size

    return count > 0
  }

  override def rmr(path: String, skipTrash: Boolean): Int = ???

  override def copyFromLocal(src: String, dst: String): Int = ???

  // ---------------------------------------------------
  // Helper methods.
  // ---------------------------------------------------

  def createTopic(inputTopic: String, partitions: Int = 1): Unit = {
    val dirName = config.getString(s"system.$configKey.path.home")
    val zk_connections = config.getString(s"system.$configKey.config.server.zookeeper.connect")

    if (!exists(inputTopic)) {
      shell !! s"""$dirName/bin/kafka-topics.sh --create --zookeeper $zk_connections --replication-factor 1 --partitions $partitions --topic $inputTopic"""
    } else {
      logger.info(s"Kafka topic $inputTopic already exists")
    }
  }

  /** Helper trait that modifies the default behavior of the `SystemConfig` instance. */
  trait ReplicateServerConfigs {
    self: SystemConfig =>

    /** Extends the default `SystemConfig` by replicating the `server.properties` for each configured host. */
    override def update(): Boolean = {
      val hasChanged = (for (e <- entries) yield e.update(config)).exists(identity)

      val conf = config.getString(s"system.$configKey.path.config")
      for ((host, id) <- hosts.zipWithIndex) {
        shell ! s""" sed "s/BROKER_ID/${id + 1}/" $conf/server.properties > $conf/server-$host.properties """
      }

      hasChanged
    }
  }
}
