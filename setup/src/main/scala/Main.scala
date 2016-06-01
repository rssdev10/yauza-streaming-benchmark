import java.io.File
import java.net.URL

import scala.io.Source
import scala.language.postfixOps
import scala.sys.process._

object Main {
  private val apacheMirror = getApacheMirror

  //println("ls -l" !)

  val VER = Map(
    "flink" -> "1.0.3",
    "kafka" -> "0.9.0.1",
    "scala" -> "2.11",
    "storm" -> "1.0.1",
    "spark" -> "1.5.1"
  ).map {case (k,v) => (k, scala.util.Properties.envOrElse(k, v))}

  val products: Map[String, Product] = Map(
    "flink" -> new Product(
      s"""flink-${VER("flink")}""",
      s"""flink-${VER("flink")}-bin-hadoop27-scala_${VER("scala")}.tgz""",
      s"""$apacheMirror/flink/flink-${VER("flink")}"""
    ) {
      override def start: Unit = {
        startIfNeeded("org.apache.flink.runtime.jobmanager.JobManager", "Flink", 1, s"""$dirName/bin/start-local.sh""")
      }

      override def stop: Unit = {
        s"""$dirName/bin/stop-local.sh""" !
      }
    },

    "spark" -> new Product(
      s"""spark-${VER("spark")}-bin-hadoop2.6""",
      s"""spark-${VER("spark")}-bin-hadoop2.6.tgz""",
      s"""$apacheMirror/spark/spark-${VER("spark")}""") {
      override def start: Unit = {

      }

      override def stop: Unit = {

      }
    },

    "kafka" -> new Product(
      s"""kafka_${VER("scala")}-${VER("kafka")}""",
      s"""kafka_${VER("scala")}-${VER("kafka")}.tgz""",
      s"""$apacheMirror/kafka/${VER("kafka")}""") {
      override def start: Unit = {

      }

      override def stop: Unit = {

      }
    },

    "storm" -> new Product(
      s"""apache-storm-${VER("storm")}""",
      s"""apache-storm-${VER("storm")}.tar.gz""",
      s"""$apacheMirror/storm/apache-storm-${VER("storm")}""") {
      override def start: Unit = {

      }

      override def stop: Unit = {

      }
    }
  )

  val scenario: Map[String, () => Unit] = Map(
    "setup" -> (() => {
      //println(apacheMirror)
      // download all products
      products.foreach { case (k, v) => if (v.urlPath.nonEmpty) v.downloadAndUntar() }
    }),
    "test_flink" -> (() => {
      // try to run Flink
      products("flink").start
      Thread sleep 30000
      products("flink").stop
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
  }

  private def getApacheMirror(): String = {
    val str = Source.fromURL("https://www.apache.org/dyn/closer.cgi").mkString
    ("""<strong>(.+)</strong>""".r).findFirstMatchIn(str).get.group(1)
  }

  def pidBySample(sample: String): String = try {
    val line = ("ps -aef" !!)
    if (line.nonEmpty)
      line.split("\n").find(str => str.contains(sample)).head.split(" ").filter(_.nonEmpty).apply(1)
    else
      ""
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

    def start : Unit
    def stop : Unit
  }
}
