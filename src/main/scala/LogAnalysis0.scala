import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.rdd.RDD
import scala.reflect.io.File
import scala.util.parsing.combinator.RegexParsers
import org.apache.spark.mllib.regression.LabeledPoint

// spark-submit --class LogAnalysis --master yarn-client SparkScalaWordCount/target/scala-2.10/sparkscalawordcount_2.10-1.0.jar --num-executors 25 2>err

// no limit on parallel jobs, but stdout is not displayed locally: need to look into worker log
// spark-submit --class LogAnalysis --master yarn-cluster SparkScalaWordCount/target/scala-2.10/sparkscalawordcount_2.10-1.0.jar --num-executors 25 2>err




/*
// spark-shell --master yarn-client --num-executors 25


import scala.util.parsing.combinator._


object Adder extends RegexParsers {
  def expr: Parser[Int] = (
   "("~expr~"+"~expr~")" ^^ { case "("~x~"+"~y~")" => x+y }
  | number
  )

  val number: Parser[Int] = """[0-9]+""".r ^^ { _.toInt}
}

Adder.parse(Adder.expr, "(7+4)")


object LogP extends RegexParsers {
  def logline: Parser[Any] = (
    timestamp~"""INFO org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler: Application Attempt"""~ident~"is done. finalState="~ident ^^ {
       case t~_~a~_~s => Option(t, a, s)
    }
  |  "[^\n]+".r ^^ (_ => None)
  )

  val ident: Parser[String] = "[A-Za-z0-9_]+".r
  val timestamp: Parser[String] = "2015-[0-9][0-9]-[0-9][0-9] [0-9:,]+".r
}


val l1 = """2015-03-10 05:15:53,196 INFO org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler: Application Attempt appattempt_1425682538854_1748_000001 is done. finalState=FINISHED"""

val l2 = """2015-03-10 05:15:53,196 INFO org.apache.hadoop.yarn.server.resourcemanager.RMAppManager$ApplicationSummary: appId=application_1425682538854_1748,name=decompress,user=joe,queue=default,state=FINISHED,trackingUrl=http://master1:8088/proxy/application_1425682538854_1748/jobhistory/job/job_1425682538854_1748,appMasterHost=icdatasrv019.icdatacluster2,startTime=1425960925158,finishTime=1425960946325,finalStatus=FAILED"""

val l3 = """2015-03-03 17:59:51,137 INFO org.apache.hadoop.yarn.server.resourcemanager.RMAppManager$ApplicationSummary: appId=application_1424932957480_0341,name=Spark shell,user=bob,queue=default,state=FINISHED,trackingUrl=http://master1:8088/proxy/application_1424932957480_0341/A,appMasterHost=icdatasrv058.icdatacluster2,startTime=1425392421737,finishTime=1425401990053,finalStatus=SUCCEEDED"""

*/

import scala.util.parsing.combinator._

/*
abstract class LogLine extends java.io.Serializable
case class Foo(ts: String, appatt: String, state: String) extends LogLine
case class AppSummary(timestamp: String, app: String, name: String, user: String, state:String,
                      url:String, host: String, startTime: String, endTime: String, finalStatus: String) extends LogLine
case class UnknownLine() extends LogLine


// Yarn Log Parser
object LogP extends RegexParsers with java.io.Serializable {
  def logline: Parser[LogLine] = (
      timestamp~"INFO org.apache.hadoop.yarn.server.resourcemanager.RMAppManager$ApplicationSummary: appId="~ident
      ~",name="~identW
      ~",user="~ident
      ~",queue=default,state="~ident
      ~",trackingUrl="~url
      ~",appMasterHost="~ident
      ~".icdatacluster2,startTime="~ident
      ~",finishTime="~ident
      ~",finalStatus="~ident ^^ {
      case t~_~app~_~name~_~user~_~state~_~url~_~host~_~stime~_~etime~_~finalStatus =>
        AppSummary(t, app, name, user, state, url, host, stime, etime, finalStatus)
    }
    )

  val ident: Parser[String] = "[A-Za-z0-9_]+".r
  val identW: Parser[String] = "[A-Za-z0-9_ ]+".r
  val timestamp: Parser[String] = "2015-[0-9][0-9]-[0-9][0-9] [0-9:,]+".r
  val url: Parser[String] = "http://[a-zA-Z0-1.]+:[0-9]+/[a-zA-Z0-9_/]+".r
}
*/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object StatsAnalysis extends java.io.Serializable{
  def getOwners (stats: RDD[LogLine with Product with Serializable]) : RDD[(String, List[String])] = { //(app, [user])
    stats.map({
      case Status(app, user, _) => (app, List(user)) //TODO: should I deal with no-status?
    })
  }
  def getSuccessRates(stats: RDD[LogLine with Product with Serializable]): RDD[(String, Double)] = { //(user, SuccessRate)
    def statusToInt(a: LogLine) : (String, (Int, Int)) = {
      a match {
        case Status(_, user, x) => (user, (if (x == "SUCCEEDED") 1 else 0, 1))
      }
    }

    def tupleSum(a : (Int,Int), b: (Int, Int)) : (Int, Int) = {
      (a._1 + b._1, a._2 + b._2)
    }
    def calcProbabilities(a:(String, (Int, Int))) : (String, Double) ={
      (a._1, a._2._1.toDouble/a._2._2)
    }
    return stats.map(statusToInt).reduceByKey(tupleSum).map(calcProbabilities)
  }

  def joinWithApp(data: RDD[(String, List[Object])], feature: RDD[(String, Object)]) : RDD[(String, List[Object])] = {
    val nested = data.join(feature)
    def unNest(element :(String, (List[Object], Object))) : (String, List[Object]) = {
      (element._1, element._2._1 :+ element._2._2)
    }
    return nested.map(unNest)
  }

  def joinWithUser(data: RDD[(String, List[Object])], feature: RDD[(String, Object)]) : RDD[(String, List[Object])] = {
    def swapUserAndApp(element: (String, List[Object])) : (String, List[Object]) = {
      val len = element._2.length
      return (element._2(0).asInstanceOf[String], element._1:: element._2.slice(1, len))
    }
    joinWithApp(data.map(swapUserAndApp),feature).map(swapUserAndApp)
  }
}


object LogAnalysis extends java.io.Serializable {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("LogAnalysis"))
    val files = List("data/sample-resource.log")
    val lines = sc.textFile(files.mkString(","))
    //val lines = sc.textFile("hdfs:///datasets/clusterlogs/yarn-yarn-resourcemanager-master1.log*")
    val parseLine = (l: String) =>
      OutputParser.parse(OutputParser.getStatus, l).getOrElse(UnknownLine())

    val tmp = lines.map(parseLine).cache()

    //val res = "Success: " + success.count() + "\n" + "Failed: " + failed.count() + "\n" +  "Other: " + other.count()

    val stats = tmp.filter(f => f match {
      case Status(_,_,x) => x == "SUCCEEDED" || x == "FAILED" || x == "KILLED"
      case _ => false
    }).cache()

    val owners = StatsAnalysis.getOwners(stats)
    for(i <- owners.collect()) yield {println(i)}

    val rates = StatsAnalysis.getSuccessRates(stats)
    for(i <- rates.collect()) yield {println(i)}

    val debugData = sc.parallelize(List(("application_1425682538854_0454", "blabla"))) : RDD[(String, Object)]
    val myJoin = StatsAnalysis.joinWithApp(owners.asInstanceOf[RDD[(String, List[Object])]], debugData)

    for(i <- myJoin.collect()) yield {println(i)}

    val joinedRates = StatsAnalysis.joinWithUser(owners.asInstanceOf[RDD[(String, List[Object])]], rates.asInstanceOf[RDD[(String, Object)]])
    for(i <- joinedRates.collect()) yield {println(i)}
    /*
    for(i <- successRate.collect()) yield {println(i)}

    def mapToLabelled(a:LogLine) : (String, Double) = {
      a match {
        case Status(app, user, "SUCCEEDED") => (user, 1.0) //LabeledPoint(1.0,
          //new DenseVector(Array(successRate.lookup(user)(0))))
        case Status(app, user, _) => (user, 0.0) //LabeledPoint(0.0,
          //new DenseVector(Array(successRate.lookup(user)(0))))
        case _ => ("-1", -1) //LabeledPoint(-1.0, new DenseVector(Array(-1.0)))
      }
    }
    val results = stats.map(mapToLabelled)
    //val myData = stats.map(mapToLabelled)

    for(i <- results.collect()) yield {println(i)}

    val joined = results join(successRate)
    for(i <- joined.collect()) yield {println(i)}*/
   /* val mapped = tmp.map(f => f match {
      case Status(_, _, "SUCCEEDED") => ("success", 1)
      case Status(_, _, "FAILED") => ("failed", 1)
      case Status(_, _, "KILLED") => ("killed", 1)
      case Status(_, _, "partially") => ("partially", 1)
      case Status(x, _, _) => (x, 1)
    })
    val grouped = mapped.reduceByKey(_ + _)
    for(i <- grouped.collect()) yield {println(i)}
    grouped.saveAsTextFile("output")*/
    //println(tmp.collect())
    //val result = tmp.collect()
    // for(i <- result) yield {println(i)}
    //ll.saveAsTextFile("app_summaries")  // in the user's HDFS home directory

    sc.stop()
  }
}

