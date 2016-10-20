package yauza.benchmark.flink.beans.experiment

import com.typesafe.config.Config
import org.peelframework.core.beans.data.{DataSet, ExperimentOutput}
import org.peelframework.core.beans.experiment.Experiment
import org.peelframework.core.beans.system.System
import org.peelframework.flink.beans.experiment.FlinkExperiment
import org.peelframework.flink.beans.system.Flink

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, _}
import scala.concurrent.duration._
import scala.language.postfixOps

class FlinkStreamingExperiment(
                                command: String,
                                systems: Set[System],
                                runner: Flink,
                                runs: Int,
                                inputs: Set[DataSet],
                                outputs: Set[ExperimentOutput],
                                name: String,
                                config: Config) extends FlinkExperiment(command, systems, runner, runs, inputs, outputs, name, config) {

  def this(
            command: String,
            runner: Flink,
            runs: Int,
            inputs: Set[DataSet],
            outputs: Set[ExperimentOutput],
            name: String,
            config: Config) = this(command, Set.empty[System], runner, runs, inputs, outputs, name, config)

  override def run(id: Int, force: Boolean): Experiment.Run[Flink] = {
    new FlinkStreamingExperiment.SingleJobRun(id, this, force)
  }

  override def copy(name: String = name, config: Config = config) = {
    new FlinkStreamingExperiment(command, systems, runner, runs, inputs, outputs, name, config)
  }
}

object FlinkStreamingExperiment {
  class SingleJobRun (id: Int, exp: FlinkStreamingExperiment, force: Boolean) extends
    FlinkExperiment.SingleJobRun(id, exp, force) {
    override protected def runJob() = {
      val t = exp.config.getLong("experiment.streaming.timeout") seconds;
      try {
        Await.ready(future(super.runJob()), t)
      } catch {
        case e: TimeoutException =>
          logger.info(s"Experiment terminated after ${t} seconds")
          cancelJob()
      }
    }
  }
}
