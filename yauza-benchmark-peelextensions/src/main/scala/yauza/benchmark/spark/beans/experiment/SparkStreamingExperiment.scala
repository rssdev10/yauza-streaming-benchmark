package yauza.benchmark.spark.beans.experiment

import com.typesafe.config.Config
import org.peelframework.core.beans.data.{DataSet, ExperimentOutput}
import org.peelframework.core.beans.experiment.Experiment
import org.peelframework.core.beans.system.System
import org.peelframework.spark.beans.experiment.SparkExperiment
import org.peelframework.spark.beans.system.Spark

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, _}
import scala.concurrent.duration._
import scala.language.postfixOps

class SparkStreamingExperiment(
                                command: String,
                                systems: Set[System],
                                runner: Spark,
                                runs: Int,
                                inputs: Set[DataSet],
                                outputs: Set[ExperimentOutput],
                                name: String,
                                config: Config) extends SparkExperiment(command, systems, runner, runs, inputs, outputs, name, config) {

  def this(
            command: String,
            runner: Spark,
            runs: Int,
            inputs: Set[DataSet],
            outputs: Set[ExperimentOutput],
            name: String,
            config: Config) = this(command, Set.empty[System], runner, runs, inputs, outputs, name, config)

  override def run(id: Int, force: Boolean): Experiment.Run[Spark] = {
    new SparkStreamingExperiment.SingleJobRun(id, this, force)
  }

  override def copy(name: String = name, config: Config = config) = {
    new SparkStreamingExperiment(command, systems, runner, runs, inputs, outputs, name, config)
  }
}

object SparkStreamingExperiment {
  class SingleJobRun (id: Int, exp: SparkStreamingExperiment, force: Boolean) extends
    SparkExperiment.SingleJobRun(id, exp, force) {
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
