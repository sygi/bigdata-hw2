/**
 * Created by sygi on 14.03.15.
 */
import scala.util.parsing.combinator.RegexParsers
import java.io.Serializable
abstract class LogLine extends java.io.Serializable
case class Foo(ts: String, appatt: String, state: String) extends LogLine
case class AppSummary(timestamp: String, app: String, name: String, user: String, state:String,
                      url:String, host: String, startTime: String, endTime: String, finalStatus: String) extends LogLine
case class Status(app: String, user:String, status: String) extends LogLine
case class UnknownLine() extends LogLine


// Yarn Log Parser
object OutputParser extends RegexParsers with java.io.Serializable {
  def logline: Parser[AppSummary] = (
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

  def getStatus: Parser[Status] = (
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
        Status(app, user, finalStatus)
      }
        |
      (".*".r ^^ {
        case all =>
        Status(all, "", "not parsed")
      })
  )

  val ident: Parser[String] = "[A-Za-z0-9_/]+".r
  val identW: Parser[String] = """[A-Za-z0-9_ /!\-\?\.]+(.jar)?""".r
  val timestamp: Parser[String] = "2015-[0-9][0-9]-[0-9][0-9] [0-9:,]+".r
  val url: Parser[String] = "http://[a-zA-Z0-1.]+:[0-9]+/[a-zA-Z0-9_/]+".r
}