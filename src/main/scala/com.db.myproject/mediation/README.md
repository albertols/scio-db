# mediation-service

- [Introduction](#Introduction)
- [Design](#Design)
- [Modelling](#Modelling)
- [Flow](#Flow)
    - [HTTPClient](#HTTPClient)

## Introduction

- Medium Post:

## Background

- intro to State and Timer (from Beam Summit 2019) https://www.youtube.com/watch?v=Q_v5Zsjuuzg 
- great intro to SCIO Beam (from Beam Summit 2021) https://github.com/iht/scio-scala-beam-summit 
- State and Timer (from Beam Summit 2023)
  https://beamsummit.org/sessions/2023/too-big-to-fail-a-beam-pattern-for-enriching-a-stream-using-state-and-timers/


by these Beam Summit contributors:
- Kenneth Knowles: https://beamsummit.org/speakers/kenneth-knowles/
- Reza Rokni: https://2021.beamsummit.org/speakers/reza-rokni/
- Israel Herraiz: https://github.com/iht
- Tobias Kaymak: https://beamsummit.org/speakers/tobias-kaymak/

## Design

- based on KV State & Timer (S & T) pattern: https://beam.apache.org/blog/timely-processing/
- leverages https://spotify.github.io/scio/releases/migrations/v0.8.0-Migration-Guide.html#async-dofns for attaching a
  https client for reaching an endpoint (e.g: https://jsonplaceholder.typicode.com/guide/) and send the MyEventRecord,
  also known as _BusinessEventRecord_ (BER, if
  you come across this acronym, sorry, this is an open source adaptation of another DataFlow application)
- applying State (as idempotent_key in BagState ) and Timer, avoiding duplicates with same idempotent_key as long Timer
  is not flushed.

## Modelling

1. Using Avro (as new/historical
   notifications) [MyEventRecord](avro/MyEventRecord.java)
2. for HTTPResponse and HTTPRequest [notification.model](notification/model)

<br />

## Flow

1. Reading **historical_notifications** from Google Cloud Storage (GCS)
2. Reading **new_notifications** from PubSub
3. Treating **historical_notifications** , _SideInput approach is taken_ (as long as TTL is applied) for discarding
   duplicates from **new_notifications**
   against **historical_notifications**.

> HINT: A unionAll with Bounded (GCS) and Unbounded (PubSub), has been discarded, as it stalls the process (only first
> emitted Pane) and GroupByKey
(GBK) when applying state (for State And Timer), when using GlobalWindow (maybe trying other Triggers).
4. KO inValidBers as toxic in GCS
5. Saving attempted BERs in KV as State and release them when Timer expires (TTL): avoiding duplicates (race conditions
   are mitigated as distinctByKey is previously applied). Then, **new_notifications** are sent through your HTTP client.
6. Saving NOTIFICATION_RESPONSE in PubSub (e.g: for analytics as external table, as **historical_notifications**)



### StateBaseAsyncDoFn


### HTTPClient

"Bring your HTTP Client", like akka and zio here: [clients](http/clients). Implement it
as [AbstractHttpClient](http/clients/AbstractHttpClient.scala) and you can include as **httpClient**:
```
  lazy val httpClient = {
    mediationConfig.mediation.httpClientType match {
      case "akka" => new AkkaHttpClient
      case "zio"  => new ZioHttpClient
      case "yourHttpClient"  => new YourHttpClient
    }
  }
```
in [StateAsyncParDoWithHttpHandler](http/StateAsyncParDoWithHttpHandler.scala)

<br />

## Testing
### unit testing

- Understanding the State and Timer pattern keeping the idempotent state, before sending the HttpRequest,
  just run in
  Intellij [MediationServiceSpec_1_OK_2_DUPLICATES](../../../../../.run/MediationServiceSpec.1 OK and 2 SENT_OR_DUPLICATED HTTP_RESPONSE should exist in the same stream.run.xml)
  <br />

> _HINT 1_: it uses the DirectRunner in your local machine (do not worry about not having a GCP project).

> _HINT 2_: PubSub emulator in Java needs this
> workaround https://cloud.google.com/pubsub/docs/emulator#pubsub-emulator-java. I have not got it sorted (yet). In the
> meantime we can just get by with TestStream.

## Other insights

- BER stress test data sets (+200K) has been undertaking with the current akka config
  <br />

