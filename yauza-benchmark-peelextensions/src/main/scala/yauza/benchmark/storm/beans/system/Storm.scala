package yauza.benchmark.storm.beans.system

import com.samskivert.mustache.Mustache
import org.peelframework.core.beans.experiment.Experiment.Run
import org.peelframework.core.beans.system.Lifespan.Lifespan
import org.peelframework.core.beans.system.System
import org.peelframework.core.config.{Model, SystemConfig}
import org.peelframework.core.util.shell

import scala.collection.JavaConverters._
import scala.collection.Seq
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

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
             version: String,
             configKey: String,
             lifespan: Lifespan,
             dependencies: Set[System] = Set(),
             mc: Mustache.Compiler
           ) extends System("storm", version, configKey, lifespan, dependencies, mc) {

  def master: String = config.getString(s"system.$configKey.config.master")

  def slaves: Set[String] = config.getStringList(s"system.$configKey.config.slaves").asScala.toSet

  def hosts: Seq[String] = {
    (slaves + master).toIndexedSeq
  }

  // ---------------------------------------------------
  // System.
  // ---------------------------------------------------

  override def configuration() = SystemConfig(config, {
    val conf = config.getString(s"system.$configKey.path.config")
    List(
      SystemConfig.Entry[Model.Yaml]( s"system.$configKey.config.server", s"$conf/storm.yaml", templatePath("conf/storm.yaml"), mc)
    )
  })

  override protected def start(): Unit = {
    val user = config.getString(s"system.$configKey.user")
    val stormPath = config.getString(s"system.$configKey.path.home")
    val log = config.getString(s"system.$configKey.config.server.storm.log.dir")
    val local = config.getString(s"system.$configKey.config.server.storm.local.dir")
    val timeout: Int = config.getString(s"system.$configKey.startup.timeout").toInt

    val initServerDirsCmd = (host: String) => {
      val initDirs = Seq(log, local) // directories to be initialized on each host
      val cmd = initDirs.map(path => s"rm -Rf $path && mkdir -p $path").mkString(" && ")
      s""" ssh $user@$host "$cmd" """
    }

    def runAndWaitPids(host:String, waitFor: Seq[String]): Seq[(String, String, Int)] = {
      logger.info(s"Initializing Storm directories at $host")
      shell.!(initServerDirsCmd(host), s"Unable to initialize Storm directories at $host.")

      waitFor.foreach(service => {
        val cmd = s"$stormPath/bin/storm $service"
        shell.!(s""" ssh $user@$host 'nohup $cmd >/dev/null 2>/dev/null & echo $$!' """, s"Unable to start Storm $service server on host '$host'.")
      })

      logger.info(s"Checking health of Storm services at $host")
      Thread.sleep(timeout * 1000)

      waitFor.map(service => (master, service, sshFindStormComponentPid(master, service)))
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
          logger.info(s"Storm $service started on host '$host'")
        else
          logger.error(s"Storm $service failed on host '$host'")
      }
    }
  }

  override protected def stop(): Unit = {
    val user = config.getString(s"system.$configKey.user")
    val timeout: Int = config.getString(s"system.$configKey.startup.timeout").toInt

    logger.info("Stopping storm processes on all hosts")
    val futureKillProcess = Future.traverse(hosts)(host => Future {
      var pids = sshFindStormComponentsPid(host)
      if (pids.nonEmpty) {
        shell.!( s"""ssh $user@$host """" + pids.map(pid => s"kill $pid;").mkString(" ") + """"""")

        Thread.sleep(timeout * 1000 / 2)

        pids = sshFindStormComponentsPid(host)
        if (pids.nonEmpty) {
          shell.!( s"""ssh $user@$host """" + pids.map(pid => s"kill -9 $pid;").mkString(" ") + "\"")
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

  override def beforeRun(run: Run[System]): Unit = {
    // handle dependencies
    for (s <- dependencies) {
      s.beforeRun(run)
    }
  }

  override def afterRun(run: Run[System]): Unit = {
    // handle dependencies
    for (s <- dependencies) {
      s.afterRun(run)
    }
  }

  // ---------------------------------------------------
  // Helper methods.
  // ---------------------------------------------------

  def sshFindPidByTemplate(host:String, template: String, num: Int = 1):Seq[Int] = {
    val user = config.getString(s"system.$configKey.user")
    val pidList: Array[String] =
      shell.!!(s"""ssh $user@$host "ps -aef | grep -iE '$template' | grep -v 'grep' | awk '{print \\$$2}' | head -$num"""")
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
    val pids = sshFindPidByTemplate(host, s"daemon\\.name=$component")
    if (pids.nonEmpty)
      pids.head
    else
      0
  }

  private def sshFindStormComponentsPid(host:String):Seq[Int] =
    sshFindPidByTemplate(host, "daemon\\.name=(nimbus|supervisor|ui|logviewer)", 10)

}
