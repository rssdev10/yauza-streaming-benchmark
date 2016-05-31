import java.io.File
import java.net.URL

import scala.io.Source
import scala.language.postfixOps
import scala.sys.process._

object Main extends App {
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
        println(s"""$dirName/bin/start-local.sh""" !!)
      }

      override def stop: Unit = {
        println(s"""$dirName/bin/stop-local.sh""" !!)
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

  // download all products
  products.foreach{case (k,v) => v.downloadAndUntar()}

  // try to run Flink
  products("flink").start
  products("flink").stop

  //println(apacheMirror)

  private def getApacheMirror(): String = {
    val str = Source.fromURL("https://www.apache.org/dyn/closer.cgi").mkString
    ("""<strong>(.+)</strong>""".r).findFirstMatchIn(str).get.group(1)
  }

  abstract class Product(val dirName: String, val fileName: String, val urlPath: String){
    def downloadAndUntar() = {
      val localFile = s"download-cache/$fileName"
      val url = urlPath + "/" + fileName

      println(s"Download $url")
      println(s"Saving to $fileName")

      val file = new File(localFile)
      if (file.exists() && file.length() > 0) {
        println(s"Using cached File $fileName")
      } else {
        new File("download-cache").mkdir
        new URL(url) #> new File(localFile) !!
      }

      val tar = "tar -xzvf " + System.getProperty("user.dir") + "/" + localFile
      println(tar !!)
    }

    def start : Unit
    def stop : Unit
  }
}
