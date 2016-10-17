package config.fixtures

import com.typesafe.config.ConfigFactory
import org.peelframework.core.beans.data.{CopiedDataSet, DataSet, ExperimentOutput, GeneratedDataSet}
import org.peelframework.core.beans.experiment.ExperimentSequence.SimpleParameters
import org.peelframework.core.beans.experiment.{ExperimentSequence, ExperimentSuite}
import org.peelframework.flink.beans.experiment.FlinkExperiment
import org.peelframework.flink.beans.job.FlinkJob
import org.peelframework.flink.beans.system.Flink
import org.peelframework.hadoop.beans.system.HDFS2
import org.peelframework.spark.beans.experiment.SparkExperiment
import org.peelframework.spark.beans.system.Spark
import org.springframework.context.annotation.{Bean, Configuration}
import org.springframework.context.{ApplicationContext, ApplicationContextAware}

/** `Yauza-benchmark` experiment fixtures for the 'yauza-benchmark' bundle. */

@Configuration
class benchmark extends ApplicationContextAware {

  /* The enclosing application context. */
  var ctx: ApplicationContext = null

  def setApplicationContext(ctx: ApplicationContext): Unit = {
    this.ctx = ctx
  }

  // ---------------------------------------------------
  // Data Generators
  // ---------------------------------------------------

  // ---------------------------------------------------
  // Data Sets
  // ---------------------------------------------------

  // ---------------------------------------------------
  // Experiments
  // ---------------------------------------------------

  @Bean(name = Array("benchmark.default"))
  def `benchmark.default`: ExperimentSuite = {
    val `benchmark.flink.default` = new FlinkExperiment(
      name    = "benchmark.flink.default",
      command =
        """
          |-v -c yauza.benchmark.flink.FlinkWC                      \
          |${app.path.apps}/yauza-benchmark-flink-jobs-1.0-SNAPSHOT.jar          \
          |${system.hadoop-2.path.input}/rubbish.txt                            \
          |${system.hadoop-2.path.output}/wordcount
        """.stripMargin.trim,
      config  = ConfigFactory.parseString(""),
      runs    = 3,
      runner  = ctx.getBean("flink-1.0.3", classOf[Flink]),
      inputs  = Set.empty,
      outputs = Set.empty
    )

    val `benchmark.spark.default` = new SparkExperiment(
      name    = "benchmark.spark.default",
      command =
        """
          |--class yauza.benchmark.spark.SparkWC                    \
          |${app.path.apps}/yauza-benchmark-spark-jobs-1.0-SNAPSHOT.jar          \
          |${system.hadoop-2.path.input}/rubbish.txt                            \
          |${system.hadoop-2.path.output}/wordcount
        """.stripMargin.trim,
      config  = ConfigFactory.parseString(""),
      runs    = 3,
      runner  = ctx.getBean("spark-2.0.0", classOf[Spark]),
      inputs  = Set.empty,
      outputs = Set.empty
    )

    new ExperimentSuite(Seq(
      `benchmark.flink.default`,
      `benchmark.spark.default`))
  }

  @Bean(name = Array("benchmark.scale-out"))
  def `benchmark.scale-out`: ExperimentSuite = {
    val `benchmark.flink.prototype` = new FlinkExperiment(
      name    = "benchmark.flink.__topXXX__",
      command =
        """
          |-v -c yauza.benchmark.flink.FlinkWC                      \
          |${app.path.apps}/yauza-benchmark-flink-jobs-1.0-SNAPSHOT.jar          \
          |${system.hadoop-2.path.input}/rubbish.txt                            \
          |${system.hadoop-2.path.output}/wordcount
        """.stripMargin.trim,
      config  = ConfigFactory.parseString(
        """
          |system.default.config.slaves            = ${env.slaves.__topXXX__.hosts}
          |system.default.config.parallelism.total = ${env.slaves.__topXXX__.total.parallelism}
          |datagen.dictionary.dize                 = 10000
          |datagen.tuples.per.task                 = 10000000 # ~ 100 MB
          |datagen.data-distribution               = Uniform
        """.stripMargin.trim),
      runs    = 3,
      runner  = ctx.getBean("flink-1.0.3", classOf[Flink]),
      inputs  = Set.empty,
      outputs = Set.empty
    )

    val `benchmark.spark.prototype` = new SparkExperiment(
      name    = "benchmark.spark.__topXXX__",
      command =
        """
          |--class yauza.benchmark.spark.SparkWC                    \
          |${app.path.apps}/yauza-benchmark-spark-jobs-1.0-SNAPSHOT.jar          \
          |${system.hadoop-2.path.input}/rubbish.txt                            \
          |${system.hadoop-2.path.output}/wordcount
        """.stripMargin.trim,
      config  = ConfigFactory.parseString(
        """
          |system.default.config.slaves            = ${env.slaves.__topXXX__.hosts}
          |system.default.config.parallelism.total = ${env.slaves.__topXXX__.total.parallelism}
          |datagen.dictionary.dize                 = 10000
          |datagen.tuples.per.task                 = 10000000 # ~ 100 MB
          |datagen.data-distribution               = Uniform
        """.stripMargin.trim),
      runs    = 3,
      runner  = ctx.getBean("spark-2.0.0", classOf[Spark]),
      inputs  = Set.empty,
      outputs = Set.empty
    )

    new ExperimentSuite(
      new ExperimentSequence(
        parameters = new SimpleParameters(
          paramName = "topXXX",
          paramVals = Seq("top005", "top010", "top020")),
        prototypes = Seq(
          `benchmark.flink.prototype`,
          `benchmark.spark.prototype`)))
  }
}
