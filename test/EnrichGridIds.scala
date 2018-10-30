
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

  val csvFileName = "test/NPO-Contacts.csv"

  val regex = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)" // split on comma, ignore commas inside quotes
  val limit = -1 // -1 -> keep empty values

  val n = 100 // how many tests

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  val wsConfig = new DefaultAsyncHttpClientConfig.Builder().build()
  val client: WSClient = AhcWSClient(wsConfig)

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

  val result = awaitSuccess(futures, List()) match {
    case Left(error) => Logger.error(error.getLocalizedMessage); Nil
    case Right(x) => x
  }

  result.flatMap {
    case (institution, Some(id), email) => List((institution, id, email))
    case (institution, None, _) =>
      Logger.error(s"could not find id for $institution")
      Nil
  }

  result.foreach(println)

  def loadGridId(institution: String): Future[Option[String]] = {
    val apiKey = "e53584d127d0aa0298c89144385a6825a20a6a2536f08ac1"
    val baseUrl = "https://springer-grid.uberresearch.com/"
    val api = "api/autocomplete"

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
            Logger.error(s"multiple results found for $institution")
            None
          }
        case _ =>
          Logger.error(s"An error occurred while fetching the Grid ID for $institution")
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
