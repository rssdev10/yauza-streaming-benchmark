import java.io.{File, FileWriter}
import java.net.URL
import java.util.Calendar

import yauza.benchmark.ResultsCollector
import yauza.benchmark.common.Config

import scala.io.Source
import scala.language.postfixOps
import scala.sys.process._

object YauzaSetup {

  private val TIME_OF_TEST: Int = 120 * 1000 /* in ms */

  private val apacheMirror = getApacheMirror

  private val curDir = System.getProperty("user.dir")

  private val props = new Config("conf/benchmark.conf")

  private val inputTopic = Config.INPUT_TOPIC_NAME

  //println("ls -l" !)
  object Product extends Enumeration {
    val flink = "flink"
    val kafka = "kafka"
    val zookeeper = "zookeeper"
    val scala_bin = "scala"
    val storm = "storm"
    val spark = "spark"
    val hadoop = "hadoop"

    val delay = "delay"

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
        startIfNeeded("org.apache.spark.deploy.master.Master", "SparkMaster", 5, s"""$dirName/sbin/start-master.sh -h localhost -p 7077""")
        startIfNeeded("org.apache.spark.deploy.worker.Worker", "SparkSlave", 5, s"""$dirName/sbin/start-slave.sh spark://localhost:7077""")
      }

      override def stop: Unit = {
        stopIfNeeded("org.apache.spark.deploy.master.Master", "SparkMaster")
        stopIfNeeded("org.apache.spark.deploy.worker.Worker", "SparkSlave")
      }
    },

    kafka -> new Product(
      s"""kafka_${VER(scala_bin)}-${VER(kafka)}""",
      s"""kafka_${VER(scala_bin)}-${VER(kafka)}.tgz""",
      s"""$apacheMirror/kafka/${VER(kafka)}""") {
      override def start: Unit = {
        val ZK_CONNECTIONS = props.getProperties.getProperty(Config.PROP_ZOOKEEPER)
        val PARTITIONS = 1

        startIfNeeded("kafka.Kafka", kafka, 10,
          s"$dirName/bin/kafka-server-start.sh", s"$dirName/config/server.properties")

        val count = s"""$dirName/bin/kafka-topics.sh --describe --zookeeper $ZK_CONNECTIONS --topic $inputTopic 2>/dev/null""" #| s"grep -c $inputTopic" !

        if (count.toInt == 0) {
          s"""$dirName/bin/kafka-topics.sh --create --zookeeper $ZK_CONNECTIONS --replication-factor 1 --partitions $PARTITIONS --topic $inputTopic""" !
        } else {
          println(s"Kafka topic $inputTopic already exists")
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

      override def config(phase:String): Unit = {
        Process(Seq("bash","-c",s"""cp -f conf/hadoop/* $dirName/etc/hadoop""")).!;
        val javaHome = System.getenv("JAVA_HOME")
        if (!javaHome.isEmpty) {
          val fw = new FileWriter(s"$dirName/etc/hadoop/hadoop-env.sh", true)
          fw.write(s"\nexport JAVA_HOME=$javaHome\n")
          fw.close()
        }
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
        s"""java -jar $dirName/$fileName --mode load_to_kafka --topic $inputTopic --bootstrap.servers localhost:9092""" !;
        println("Data uploaded to kafka")
        Thread sleep 10000
      }

      override def stop: Unit = {
        stopIfNeeded(fileName, benchmark_flink)
      }

      override def config(phase:String): Unit = {
        if (!phase.equalsIgnoreCase("prepare")) return

        s"""java -jar $dirName/$fileName --mode generate_file""" !
      }
    },

    benchmark_flink -> new Product(
      "./bin",
      s"""benchmark-flink-${VER(benchmark)}-all.jar""",
      "") {
      override def start: Unit = {
        startIfNeeded(fileName, benchmark_flink, 10,
          products(flink).dirName + "/bin/flink",
          s"""run $dirName/$fileName --config conf/benchmark.conf"""
        )
      }

      override def stop: Unit = {
        stopIfNeeded(fileName, benchmark_flink)
      }
    },

    delay -> new Product(delay, "", "") {
      override def start: Unit = {
        Thread sleep 30 * 1000
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
          v.config("setup")
        } catch {
          case _: Throwable =>
        }
      }
    }),

    "test_data_prepare" -> (() => {
      products(hadoop).config("prepare")
      products(hadoop).start

      products(datagenerator).config("prepare")

      products(hadoop).stop
    }),

    "test_flink" -> (() => {
      // try to run Flink
      val seq = Array(
        zookeeper,
        hadoop,
        kafka,
        flink,

        delay,

        datagenerator,

        benchmark_flink
      )
      //seq.foreach(products(_).start)
      seq.foreach(x => {
        println(Calendar.getInstance.getTime + ": " + x + " starting ******************")
        products(x).start
      })

      Thread sleep TIME_OF_TEST

      products(benchmark_flink).stop

      println(Calendar.getInstance.getTime + ": " + " Processing of results starting ******************")
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
      println( name + " is already running...")
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
      if (exists && file.length() > 1024) {
        // check minimal size on case of HTTP-redirection with saving of non zero file
        println(s"Using cached File $fileName")
      } else {
        if (exists) file.delete()
        else new File("download-cache").mkdir

        new URL(url) #> new File(localFile) !
      }

      val tar = "tar -xzvf " + curDir + "/" + localFile
      tar !
    }

    def config(phase:String) : Unit = {}

    def start : Unit = {}
    def stop : Unit = {}
  }
}
