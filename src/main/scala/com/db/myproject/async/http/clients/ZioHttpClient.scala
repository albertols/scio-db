package com.db.myproject.async.http.clients

import zio._
import zio.http._

/**
 * WIP
 */
object ZioHttpClient extends ZIOAppDefault {
  val url = URL.decode("http://sports.api.decathlon.com/groups/water-aerobics").toOption.get

  val program = for {
    client <- ZIO.service[Client]
    res    <- client.url(url).get("/")
    data   <- res.body.asString
    _      <- Console.printLine(data)
  } yield ()

  override val run = program.provide(Client.default, Scope.default)


 // type HttpClient = Has[HttpClient.Service]
//
 // trait Service {
 //   def send(request: DeviceRequest): Task[SendNotificationLastDeviceResponseDto]
 // }
//
 // val live: Layer[Nothing, HttpClient] = ZLayer.fromManaged {
 //   val managedHttpClient: ZManaged[Any, Throwable, Client[Task]] = BlazeClientBuilder[Task](ExecutionContext.global).resource.toManagedZIO
//
 //   managedHttpClient.map { httpClient =>
 //     new Service with Http4sClientDsl[Task] {
 //       override def send(request: DeviceRequest): Task[SendNotificationLastDeviceResponseDto] = {
 //         val httpRequest = Request[Task](Method.POST, uri"https://reqbin.com/sample/post/json")
 //           .withEntity(request)
//
 //         httpClient.expect[SendNotificationLastDeviceResponseDto](httpRequest)
 //       }
 //     }
 //   }
 // }

  //def send(request: DeviceRequest): RIO[HttpClient, SendNotificationLastDeviceResponseDto] =
  //  ZIO.accessM(_.get.send(request))
}

