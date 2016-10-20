/**
 * Copyright (C) 2016 TU Berlin (peel@dima.tu-berlin.de)
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
package yauza.benchmark.storm.beans.experiment

import java.io.FileWriter
import java.nio.file._

import com.typesafe.config.Config
import org.peelframework.core.beans.data.{DataSet, ExperimentOutput}
import org.peelframework.core.beans.experiment.Experiment
import org.peelframework.core.beans.system.System
import org.peelframework.core.util.shell
import spray.json._
import yauza.benchmark.storm.beans.system.Storm

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps

/** An `Expriment` implementation which handles the execution of a single Storm job. */
class StormExperiment(
    command: String,
    systems: Set[System],
    runner : Storm,
    runs   : Int,
    inputs : Set[DataSet],
    outputs: Set[ExperimentOutput],
    name   : String,
    config : Config) extends Experiment(command, systems, runner, runs, inputs, outputs, name, config) {

  def this(
    command: String,
    runner : Storm,
    runs   : Int,
    inputs : Set[DataSet],
    outputs: Set[ExperimentOutput],
    name   : String,
    config : Config) = this(command, Set.empty[System], runner, runs, inputs, outputs, name, config)

  override def run(id: Int, force: Boolean): Experiment.Run[Storm] = {
    new StormExperiment.SingleJobRun(id, this, force)
  }

  def copy(name: String = name, config: Config = config) = {
    new StormExperiment(command, systems, runner, runs, inputs, outputs, name, config)
  }
}

object StormExperiment {

  case class State(
    command         : String,
    runnerID        : String,
    runnerName      : String,
    runnerVersion   : String,
    var runExitCode : Option[Int] = None,
    var runTime     : Long = 0) extends Experiment.RunState

  object StateProtocol extends DefaultJsonProtocol with NullOptions {
    implicit val stateFormat = jsonFormat6(State)
  }

  /**
   * A private inner class encapsulating the logic of single run.
   */
  class SingleJobRun(val id: Int, val exp: StormExperiment, val force: Boolean) extends Experiment.SingleJobRun[Storm, State] {

    import StormExperiment.StateProtocol._

    val runnerLogPath = exp.config.getString(s"system.${exp.runner.configKey}.path.log")

    override def isSuccessful = state.runExitCode.getOrElse(-1) == 0

    override protected def loadState(): State = {
      if (Files.isRegularFile(Paths.get(s"$home/state.json"))) {
        try {
          scala.io.Source.fromFile(s"$home/state.json").mkString.parseJson.convertTo[State]
        } catch {
          case e: Throwable => State(command, exp.runner.beanName, exp.runner.name, exp.runner.version)
        }
      } else {
        State(command, exp.runner.beanName, exp.runner.name, exp.runner.version)
      }
    }

    override protected def writeState() = {
      val fw = new FileWriter(s"$home/state.json")
      fw.write(state.toJson.prettyPrint)
      fw.close()
    }

    override protected def runJob() = {
      val execTime = exp.config.getLong("experiment.streaming.timeout") seconds;
      try {
        def runJobFunc: (Int, Long) = Experiment.time(this ! (command, s"$home/run.out", s"$home/run.err"))
        // try to execute the experiment run plan
        val (runExit, t) = Await.result(future(runJobFunc), execTime)
        state.runTime = t
        state.runExitCode = Some(runExit)
      } catch {
        case e: TimeoutException =>
          logger.info(s"Experiment terminated after ${execTime} seconds")
          cancelJob()
          state.runExitCode = Some(0)
      }
    }

    override def cancelJob() = {

    }

    private def !(command: String, outFile: String, errFile: String) = {
      shell ! s"${exp.config.getString(s"system.${exp.runner.configKey}.path.home")}/bin/storm ${command.trim} > $outFile 2> $errFile"
    }
  }
}
