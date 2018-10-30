
import java.io.{File, PrintWriter}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.asynchttpclient.DefaultAsyncHttpClientConfig
import play.api.Logger
import play.api.http.Status
import play.api.libs.json.{Format, Json}
import play.api.libs.ws.ahc.AhcWSClient
import play.api.libs.ws.WSClient

import scala.annotation.tailrec
import scala.concurrent.{Await, Future}
import scala.io.Source
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

case class GridInformation(id: String, name: String, city: String, state: Option[String], country: String)

object GridInformation {
  implicit val format: Format[GridInformation] = Json.format[GridInformation]
}

object EnrichGridIds extends App {

  // Input file setting
  val csvFileName = "test/NPO-Contacts.csv"
  val regex = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)" // split on comma, ignore commas inside quotes
  val limit = -1 // -1 -> keep empty values

  val n = 10 // how many tests

  // Disambiguation setting

  // if true, the most specific result is used if there are multiple results
  // if false, multiple results are discarded
  val lenientMultiple = true

  val apiKey = "e53584d127d0aa0298c89144385a6825a20a6a2536f08ac1"
  val baseUrl = "https://springer-grid.uberresearch.com/"
  val api = "api/autocomplete"

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  val wsConfig = new DefaultAsyncHttpClientConfig.Builder().build()
  val client: WSClient = AhcWSClient(wsConfig)

  // Output file setting
  val outputFileName = "output.csv"

  val lines = Source.fromFile(csvFileName)
    .getLines
    .drop(1) // header
    .take(n) // only for test
    .toSeq

  val futures = lines
    .map { line =>
      line.split(regex, limit)
    }
    .flatMap {
      // institution,country,contact_name,dept,email,fax,job,mobile,other,tel
      case arr if arr.length == 10 => List((arr(0), arr(4))) // instituation, email
      case arr => Logger.error(s"wrong length of line ${arr(0)}"); None
    }
    .map {
      case (institution, email) => loadGridId(institution).map(id => (institution, id, email))
    }

  val rawResult = awaitSuccess(futures, List()) match {
    case Left(error) => Logger.error(error.getLocalizedMessage); Nil
    case Right(x) => x
  }

  val results = rawResult.flatMap {
    case (institution, Some(id), email) => List((institution, id, email))
    case (institution, None, _) =>
      Logger.error(s"could not find id for $institution")
      Nil
  }

  val pw = new PrintWriter(new File(outputFileName))
  results.foreach(result => pw.write(s"${result._1},${result._2},${result._3}"))
  pw.close
  def loadGridId(institution: String): Future[Option[String]] = {


    val url = s"$baseUrl$api?q=$institution"

    //curl -H 'api_key: e53584d127d0aa0298c89144385a6825a20a6a2536f08ac1' \
    //-H 'Accept: application/json; version=2' \
    //'https://springer-grid.uberresearch.com/api/autocomplete?q=University+of+Oxf'

    client.url(url).withHeaders(("api_key", apiKey), ("Accept", "application/json; version=2")).get().map { response =>
      response.status match {
        case Status.OK =>
          val gridInformations = response.json.as[Seq[GridInformation]]
          if (gridInformations.size == 1) {
            Some(gridInformations.head.id)
          } else if (gridInformations.size == 0) {
            Logger.error(s"no result found for $institution")
            None
          } else {
            if (lenientMultiple) {
              Some(gridInformations.head.id)
            } else {
              Logger.error(s"multiple results found for $institution")
              None
            }
          }
        case e =>
          Logger.error(s"An error occurred while fetching the Grid ID for $institution: ${e}")
          Logger.error(response.statusText)
          Logger.error(response.body)

          None
      }
    }
  }

  @tailrec private def awaitSuccess[A](fs: Seq[Future[A]], done: Seq[A] = Seq()):
  Either[Throwable, Seq[A]] = {
    val first = Future.firstCompletedOf(fs)
    Await.ready(first, Duration.Inf).value match {
      case None => awaitSuccess(fs, done)
      case Some(Failure(e)) => Left(e)
      case Some(Success(_)) =>
        val (complete, running) = fs.partition(_.isCompleted)
        val answers = complete.flatMap(_.value)
        answers.find(_.isFailure) match {
          case Some(Failure(e)) => Left(e)
          case _ =>
            if (running.length > 0) {
              awaitSuccess(running, answers.map(_.get) ++: done)
            } else {
              Right( answers.map(_.get) ++: done )
            }
        }
    }
  }
}
