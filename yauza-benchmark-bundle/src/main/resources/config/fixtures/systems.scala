package config.fixtures

import yauza.benchmark.kafka.beans.system.Kafka
import yauza.benchmark.storm.beans.system.Storm
import yauza.benchmark.data.beans.system.StreamGenerator
import com.samskivert.mustache.Mustache
import com.samskivert.mustache.Mustache.Compiler
import org.peelframework.core.beans.system.{Lifespan, System}
import org.peelframework.dstat.beans.system.Dstat
import org.peelframework.flink.beans.system.Flink
import org.peelframework.hadoop.beans.system.HDFS2
import org.peelframework.spark.beans.system.Spark
import org.peelframework.zookeeper.beans.system.Zookeeper
import org.springframework.context.annotation.{Bean, Configuration}
import org.springframework.context.{ApplicationContext, ApplicationContextAware}

/** System beans for the 'yauza.benchmark' bundle. */
@Configuration
class systems extends ApplicationContextAware {

  /* The enclosing application context. */
  var ctx: ApplicationContext = null

  def setApplicationContext(ctx: ApplicationContext): Unit = {
    this.ctx = ctx
  }

  // ---------------------------------------------------
  // Systems
  // ---------------------------------------------------

  @Bean(name = Array("flink-1.0.3"))
  def `flink-1.0.3`: Flink = new Flink(
    version      = "1.0.3",
    configKey    = "flink",
    lifespan     = Lifespan.EXPERIMENT,
    dependencies = Set(
                        ctx.getBean("kafka-0.8.2.2", classOf[Kafka]),
                        ctx.getBean("dstat-0.7.2", classOf[Dstat]),
                        ctx.getBean("stream-1.0.0", classOf[StreamGenerator])),

    mc           = ctx.getBean(classOf[Compiler])
  )

  @Bean(name = Array("spark-2.0.0"))
  def `spark-2.0.0`: Spark = new Spark(
    version      = "2.0.0",
    configKey    = "spark",
    lifespan     = Lifespan.EXPERIMENT,
    dependencies = Set(
                        ctx.getBean("kafka-0.8.2.2", classOf[Kafka]),
                        ctx.getBean("dstat-0.7.2", classOf[Dstat]),
                        ctx.getBean("stream-1.0.0", classOf[StreamGenerator])),

    mc           = ctx.getBean(classOf[Mustache.Compiler])
  )

  @Bean(name = Array("kafka-0.8.2.2"))
  def `kafka-0.8.2.2`: Kafka = new Kafka(
    version      = "0.8.2.2",
    configKey    = "kafka",
    lifespan     = Lifespan.RUN,
    dependencies = Set(ctx.getBean("zookeeper-3.4.5", classOf[Zookeeper])),
    mc           = ctx.getBean(classOf[Mustache.Compiler])
  )

  @Bean(name = Array("storm-1.0.2"))
  def `storm-1.0.2`: Storm = new Storm(
    version      = "1.0.2",
    configKey    = "storm",
    lifespan     = Lifespan.EXPERIMENT,
    dependencies = Set(
                      ctx.getBean("zookeeper-3.4.5", classOf[Zookeeper]),
                      ctx.getBean("kafka-0.8.2.2", classOf[Kafka]),
                      ctx.getBean("dstat-0.7.2", classOf[Dstat]),
                      ctx.getBean("stream-1.0.0", classOf[StreamGenerator])),

    mc           = ctx.getBean(classOf[Mustache.Compiler])
  )

  @Bean(name = Array("dstat-0.7.2"))
  def `dstat-0.7.2`: Dstat = new Dstat(
    version      = "0.7.2",
    configKey    = "dstat",
    lifespan     = Lifespan.RUN,
    dependencies = Set.empty,
    mc           = ctx.getBean(classOf[Mustache.Compiler])
  )

  @Bean(name = Array("stream-1.0.0"))
  def `stream-1.0.0`: StreamGenerator = new StreamGenerator(
    version      = "1.0.0",
    configKey    = "benchmark",
    lifespan     = Lifespan.RUN,
    dependencies = Set.empty,
    mc           = ctx.getBean(classOf[Mustache.Compiler]),
    "yauza-benchmark-datagens-1.0-SNAPSHOT.jar"
  )
}
