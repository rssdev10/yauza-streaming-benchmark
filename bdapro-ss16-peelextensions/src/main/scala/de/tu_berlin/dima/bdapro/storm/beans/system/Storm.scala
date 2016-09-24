package de.tu_berlin.dima.bdapro.storm.beans.system

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

/** Wrapper class for storm.
  *
  * Implements storm as a Peel `System` and provides setup and teardown methods.
  *
  * @param version Version of the system (e.g. "7.1")
  * @param configKey The system configuration resides under `system.\${configKey}`
  * @param lifespan `Lifespan` of the system
  * @param dependencies Set of dependencies that this system needs
  * @param mc The moustache compiler to compile the templates that are used to generate property files for the system
  */
class Storm(
             version      : String,
             configKey    : String,
             lifespan     : Lifespan,
             dependencies : Set[System] = Set(),
             mc           : Mustache.Compiler) extends System("storm", version, configKey, lifespan, dependencies, mc)
  with DistributedLogCollection {

  // ---------------------------------------------------
  // LogCollection.
  // ---------------------------------------------------

  def master: String = config.getString(s"system.$configKey.config.master")

  def slaves: Set[String] = config.getStringList(s"system.$configKey.config.slaves").asScala.toSet

  override def hosts: Seq[String] = {
    (slaves + master).toIndexedSeq
  }

  override protected def logFilePatterns(): Seq[Regex] = {
    hosts.map(Pattern.quote).flatMap(node => Set(
      s"\\.*nimbus.log".r,
      s"\\.*supervisor.log".r)
    )
  }
  // ---------------------------------------------------
  // System.
  // ---------------------------------------------------

  override def configuration() = SystemConfig(config, {
    val conf = config.getString(s"system.$configKey.path.config")
    List(
      SystemConfig.Entry[Model.Yaml](      // render using the 'Model.Yaml' controller
        s"system.$configKey.storm.yaml",   // use this config namespace as a 'model'
        s"$conf/storm.yaml",               // instantiate the config file
        templatePath("conf/storm.yaml"),   // use this template to render the file
        mc
      )
    ) // return a list of configuration files to render on setup
  })

  override protected def start(): Unit = {
    val stormPath = config.getString(s"system.$configKey.path.home")
    val stormTmp = config.getString(s"system.$configKey.path.storm.tmp")
    val timeout: Int = config.getString(s"system.$configKey.startup.timeout").toInt

    val cmdCommon: String = Seq(
      s"rm -rf $stormTmp"
    ).mkString("; ")

    def runAndWaitPids(host:String, waitFor: Seq[String]): Seq[(String, String, Int)] = {
      shell ! s""" ssh $host '$cmdCommon'"""

      waitFor
        .map(name => s"$stormPath/bin/storm $name")
        .foreach(cmd => shell ! s""" ssh $host 'nohup $cmd >/dev/null 2>/dev/null & echo $$!' """)

      Thread.sleep(timeout * 1000)

      waitFor.map(name =>
        (master, name, sshFindStormComponentPid(master, name))
      )
    }

    logger.info("Starting storm processes on all hosts")
    val futureProcessDescriptors: Future[Seq[Seq[(String, String, Int)]]] = Future.traverse(hosts)(host => Future {
      val waitFor: Seq[String] =
        if (host.equals(master)) {
          var waitForMaster = Seq[String](
            "nimbus",
            "ui"
          )

          if (slaves.isEmpty || slaves.contains(master)) {
            waitForMaster ++= Seq("supervisor")
          }

          waitForMaster ++= Seq("logviewer")
          waitForMaster
        } else {
          Seq(
            "supervisor",
            "logviewer"
          )
        }

      runAndWaitPids(host, waitFor)
    })

    Await.result(futureProcessDescriptors, Math.max(3 * timeout, 5 * hosts.size).seconds).foreach { case (seq) =>
      seq.foreach { case (host, service, pid) =>
        if (pid > 0)
          logger.info(s"storm $service started on host '$host' with PID $pid")
        else
          logger.error(s"storm $service didn't start on host '$host'")
      }
    }
  }

  override protected def stop(): Unit = {
    logger.info("Stopping storm processes on all hosts")
    val futureKillProcess = Future.traverse(hosts)(host => Future {
      var pids = sshFindStormComponentsPid(host)
      if (pids.nonEmpty) {
        shell.!( s"""ssh $host """" + pids.map(pid => s"kill $pid;").mkString(" ") + """"""")

        Thread.sleep(5000)

        pids = sshFindStormComponentsPid(host)
        if (pids.nonEmpty) {
          shell.!( s"""ssh $host """" + pids.map(pid => s"kill -9 $pid;").mkString(" ") + "\"")
        }
      }
      logger.info(s"storm stopped on host '$host'")
    })
    Await.result(futureKillProcess, Math.max(30, 5 * hosts.size).seconds)
  }

  override def isRunning: Boolean = {
    val futureProcessDescriptors: Future[Seq[Seq[Int]]] = Future.traverse(hosts)(host => Future {
      sshFindStormComponentsPid(host)
    })
    Await.result(futureProcessDescriptors, Math.max(30, 5 * hosts.size).seconds).exists(seq => seq.exists(_ > 0))
  }

    // ---------------------------------------------------
  // Helper methods.
  // ---------------------------------------------------

  def sshFindPidByTemplate(host:String, template: String, num: Int = 1):Seq[Int] = {
    val pidList: Array[String] =
      shell.!!(s"""ssh $host "ps -aef | grep -E '$template' | grep -v 'grep' | awk '{print \\$$2}' | head -$num"""")
        .split("\n")

    pidList.map((pid: String) =>
      try {
        pid.toInt
      } catch {
        case e: NumberFormatException => 0
      }
    ).filter( _ > 0 )
  }

  private def sshFindStormComponentPid(host:String, component:String):Int = {
    val pids = sshFindPidByTemplate(host, s"daemon.name=$component")
    if (pids.nonEmpty)
      pids.head
    else
      0
  }

  private def sshFindStormComponentsPid(host:String):Seq[Int] =
    sshFindPidByTemplate(host, "daemon\\.name=(nimbus|supervisor|ui|logviewer)", 10)

}
