import scala.language.postfixOps
import sys.process._
import scala.io.Source

object Main extends App {
  private val apacheMirror = getApacheMirror

  //println("ls -l" !)

  println(scala.util.Properties.envOrElse("flink", "flink"))
  println(scala.util.Properties.envOrElse("scala", "scala"))
  println(scala.util.Properties.envOrElse("kafka", "kafka"))

  println(apacheMirror)

  private def getApacheMirror(): String = {
    val str = Source.fromURL("https://www.apache.org/dyn/closer.cgi").mkString
    ("""<strong>(.+)</strong>""".r).findFirstMatchIn(str).get.group(1)
  }
}
