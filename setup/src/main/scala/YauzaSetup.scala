import java.io.File
import java.net.URL

import yauza.benchmark.flink.ResultsCollector

import scala.io.Source
import scala.language.postfixOps
import scala.sys.process._

object YauzaSetup {

  private val TIME_OF_TEST: Int = 120 * 1000 /* in ms */

  private val apacheMirror = getApacheMirror

  //println("ls -l" !)
  object Product extends Enumeration {
    val flink = "flink"
    val kafka = "kafka"
    val zookeeper = "zookeeper"
    val scala_bin = "scala"
    val storm = "storm"
    val spark = "spark"
    val hadoop = "hadoop"

    // benchmark
    val benchmark = "benchmark"
    val datagenerator = "data-generator"
    val benchmark_flink = "benchmark-flink"
  }
  import Product._

  val VER = Map(
    // system
    flink -> "1.0.3",
    kafka -> "0.9.0.1",
    scala_bin -> "2.11",
    storm -> "1.0.1",
    spark -> "1.6.1",
    hadoop -> "2.7.2",

    // benchmark
    benchmark -> "0.1"
  ).map {case (k,v) => (k, scala.util.Properties.envOrElse(k, v))}

  val products: Map[String, Product] = Map(
    flink -> new Product(
      s"""flink-${VER(flink)}""",
      s"""flink-${VER(flink)}-bin-hadoop27-scala_${VER(scala_bin)}.tgz""",
      s"""$apacheMirror/flink/flink-${VER(flink)}"""
    ) {
      override def start: Unit = {
        startIfNeeded("org.apache.flink.runtime.jobmanager.JobManager", flink, 1, s"""$dirName/bin/start-local.sh""")
      }

      override def stop: Unit = {
        s"""$dirName/bin/stop-local.sh""" !
      }
    },

    spark -> new Product(
      s"""spark-${VER(spark)}-bin-hadoop2.6""",
      s"""spark-${VER(spark)}-bin-hadoop2.6.tgz""",
      s"""$apacheMirror/spark/spark-${VER(spark)}""") {
      override def start: Unit = {

      }

      override def stop: Unit = {

      }
    },

    kafka -> new Product(
      s"""kafka_${VER(scala_bin)}-${VER(kafka)}""",
      s"""kafka_${VER(scala_bin)}-${VER(kafka)}.tgz""",
      s"""$apacheMirror/kafka/${VER(kafka)}""") {
      override def start: Unit = {
        val ZK_CONNECTIONS = "localhost"//:9092"
        val TOPIC = "yauza_input"
        val PARTITIONS = 1

        startIfNeeded("kafka.Kafka", kafka, 10,
          s"$dirName/bin/kafka-server-start.sh", s"$dirName/config/server.properties")

        val count = s"""$dirName/bin/kafka-topics.sh --describe --zookeeper $ZK_CONNECTIONS --topic $TOPIC 2>/dev/null""" #| s"grep -c $TOPIC" !

        if (count.toInt == 0) {
          s"""$dirName/bin/kafka-topics.sh --create --zookeeper $ZK_CONNECTIONS --replication-factor 1 --partitions $PARTITIONS --topic $TOPIC""" !
        } else {
          println(s"Kafka topic $TOPIC already exists")
        }
      }

      override def stop: Unit = {
        stopIfNeeded( "kafka.Kafka", kafka)
        "rm -rf /tmp/kafka-logs/" !
      }
    },

    storm -> new Product(
      s"""apache-storm-${VER(storm)}""",
      s"""apache-storm-${VER(storm)}.tar.gz""",
      s"""$apacheMirror/storm/apache-storm-${VER(storm)}""") {
      override def start: Unit = {

      }

      override def stop: Unit = {

      }
    },

    hadoop -> new Product(
      s"""hadoop-${VER(hadoop)}""",
      s"""hadoop-${VER(hadoop)}.tar.gz""",
      s"""$apacheMirror/hadoop/common/hadoop-${VER(hadoop)}""") {
      override def start: Unit = {
        s"""$dirName/sbin/start-dfs.sh""" !
      }

      override def stop: Unit = {
        s"""$dirName/sbin/stop-dfs.sh""" !
      }

      override def config: Unit = {
        s"""cp -f conf/hadoop $dirName/etc/hadoop""" !;
        s"""$dirName/bin/hdfs namenode -format -force""" !
      }
    },

    zookeeper -> new Product(
      s"""apache-storm-${VER(storm)}""", "", "") {
      override def start: Unit = {
        startIfNeeded("dev_zookeeper", zookeeper, 10, s"$dirName/bin/storm", "dev-zookeeper")
      }

      override def stop: Unit = {
        stopIfNeeded("dev_zookeeper", zookeeper)
        "rm -rf /tmp/dev-storm-zookeeper" !
      }
    },

    datagenerator -> new Product(
      "./bin",
      s"""data-generator-${VER(benchmark)}-all.jar""",
      "") {
      override def start: Unit = {
        s"""java -jar $dirName/$fileName --mode load_to_kafka --topic yauza_input --bootstrap.servers localhost:9092""" !;
        println("Data uploaded to kafka")
        Thread sleep 10000
      }

      override def stop: Unit = {
        stopIfNeeded(fileName, benchmark_flink)
      }

      override def config: Unit = {
        s"""java -jar $dirName/$fileName --mode generate_file""" !
      }
    },

    benchmark_flink -> new Product(
      "./bin",
      s"""benchmark-flink-${VER(benchmark)}-all.jar""",
      "") {
      override def start: Unit = {
        startIfNeeded(fileName, benchmark_flink, 10, "java",
          s"""-jar $dirName/$fileName --topic yauza_input --bootstrap.servers localhost:9092""")
      }

      override def stop: Unit = {
        stopIfNeeded(fileName, benchmark_flink)
      }
    }
  )

  val scenario: Map[String, () => Unit] = Map(
    "setup" -> (() => {
      //println(apacheMirror)
      // download all products
      products.foreach { case (k, v) => if (v.urlPath.nonEmpty) v.downloadAndUntar() }
      products.foreach { case (k, v) =>
        try {
          v.config
        } catch {
          case _: Throwable =>
        }
      }
    }),

    "test_data_prepare" -> (() => {
      products(hadoop).config
      products(hadoop).start

      products(datagenerator).config

      products(hadoop).stop
    }),

    "test_flink" -> (() => {
      // try to run Flink
      val seq = Array(
        zookeeper,
        hadoop,
        kafka,
        flink ,

        datagenerator,

        benchmark_flink
      )
      seq.foreach(products(_).start)
      //seq.foreach(x => {products(x).start; println(x + " done ******************")})
      Thread sleep TIME_OF_TEST

      ResultsCollector.main(Array[String]())

//      seq.reverse.foreach(products(_).stop)
    }),

    "stop_all" -> (() => {
      val seq = Array(
        zookeeper,
        hadoop,
        kafka,
        flink,
        spark,
        storm,

        datagenerator,
        benchmark_flink
      )
      seq.reverse.foreach(products(_).stop)
    })
  )

  def main(args: Array[String]) {
    if (args.length > 0 && scenario.contains(args(0))) {
      scenario(args(0)).apply()
    } else {
      println(args.mkString(" "))
      println(
        """
          |Select command:
          | setup - download and unpack all files
          | flink_test - run test with Flink
          | ...
          | """.stripMargin)
    }
    System.exit(0);
  }

  private def getApacheMirror: String = {
    val str = Source.fromURL("https://www.apache.org/dyn/closer.cgi").mkString
    """<strong>(.+)</strong>""".r.findFirstMatchIn(str).get.group(1)
  }

  def pidBySample(sample: String): String = try {
    ("ps -aef" !!).split("\n").find(str => str.contains(sample)).head.split(" ").filter(_.nonEmpty).apply(1)
  } catch {
    case _: Throwable => ""
  }

  def startIfNeeded(sample: String, name: String, sleepTime:Integer, args: String*): Unit = {
    val pid = pidBySample(sample)
    if (pid.nonEmpty) {
      println( name + "is already running...")
    } else {
      args.mkString(" ").run()
      Thread sleep(sleepTime * 1000)
    }
  }

  def stopIfNeeded(sample: String, name: String): Unit = {
    val pid = pidBySample(sample)
    if (pid.nonEmpty) {
      s"""kill $pid""".run()
        Thread sleep 1000
      val again = pidBySample(sample)
      if (again.nonEmpty) {
        s"""kill -9 $pid""".run()
      }
    } else {
      println("No $name instance found to stop")
    }
  }

  abstract class Product(val dirName: String, val fileName: String, val urlPath: String){
    def downloadAndUntar() = {
      val localFile = s"download-cache/$fileName"
      val url = urlPath + "/" + fileName

      println(s"Download $url")
      println(s"Saving to $fileName")

      val file = new File(localFile)
      val exists = file.exists()
      if (exists && file.length() > 0) {
        println(s"Using cached File $fileName")
      } else {
        if (exists) file.delete()
        else new File("download-cache").mkdir

        new URL(url) #> new File(localFile) !
      }

      val tar = "tar -xzvf " + System.getProperty("user.dir") + "/" + localFile
      tar !
    }

    def config : Unit = {}

    def start : Unit
    def stop : Unit
  }
}
