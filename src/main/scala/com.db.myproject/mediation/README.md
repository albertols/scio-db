# mediation-service

- [Introduction](#Introduction)
- [Design](#Design)
- [Modelling](#Modelling)
- [Flow](#Flow)
    - [HTTPClient](#HTTPClient)
- [Implementation](#Implementation)
    - [StateBaseAsyncDoFn](#StateBaseAsyncDoFn)
    - [HTTPClient](#HTTPClient)
        - [AkkaHttpClient](#AkkaHttpClient)
    - [ZIORetry](#ZIORetry)
- [Testing](#Testing)
    - [StateAsyncParDoWithHttpHandler](#StateAsyncParDoWithHttpHandler)
- [StressTests](#StressTests)
- [Background](#Background)

## Introduction

- Medium Post:

## Design

- based on KV State & Timer (S & T) pattern: https://beam.apache.org/blog/timely-processing/
- leverages https://spotify.github.io/scio/releases/migrations/v0.8.0-Migration-Guide.html#async-dofns attaching a
  https client for reaching an endpoint (e.g: https://jsonplaceholder.typicode.com/guide/) sending _MyEventRecord_,
  also known as _BusinessEventRecord_ (BER, if
  you come across this acronym, sorry, this is an open source adaptation of a productive DataFlow application)
- applying State (as idempotent_key in BagState) and Timer, avoiding duplicates with same idempotent_key as long as the
  Timer
  is not flushed (acting as TTL).

## Modelling

1. Using Avro (as new/historical
   notifications): [MyEventRecord](avro/MyEventRecord.java)
2. HTTPResponse and HTTPRequest: [notification.model](notification/model)


## Flow

This diagram represents the ingestion and processing flow of the BER notifications, from ingestion to delivery the HTTP
endpoint through its state management within the S & T:
![mediation_design.png](../../../../../docs/mediation/mediation_design.png)
1. Reading **historical_notifications** from Google Cloud Storage (GCS)
2. Reading **new_notifications** from PubSub
3. Treating **historical_notifications**, _SideInput approach is taken_ (as long as TTL is applied) for discarding
   duplicates from **new_notifications**
   against **historical_notifications**.

> HINT: A unionAll with Bounded (GCS) and Unbounded (PubSub), has been discarded, as it stalls the process (only first
> emitted Pane) and GroupByKey
(GBK) when applying state (for State And Timer), when using GlobalWindow (maybe trying other Triggers). Potential
> option, reloading **historical_notifications** into PubSub and loading into the State and Timer

4. KO inValidBers as toxic in GCS
5. Saving attempted BERs (new_notifications) in KV as State and release them when Timer expires (TTL): avoiding
   duplicates (race conditions
   are mitigated as distinctByKey is previously applied). Then, **new_notifications** are sent through your HTTP client.
6. Saving NOTIFICATION_RESPONSE in PubSub (e.g: for analytics as external table, as **historical_notifications**)

## Implementation
### StateBaseAsyncDoFn

This class is mainly based on the implementation of SCIO's
_BaseAsyncDoFn_ https://github.com/spotify/scio/blob/main/scio-core/src/main/java/com/spotify/scio/transforms/BaseAsyncDoFn.java
abstracting out some methods and adding a Timer(ttl) along with a BagState (buffer), all needed to use the S & T
pattern,
preventing duplicated HTTP Requests from being sent:

```
    public void processElement(
            @Element InputT element,
            @TimerId("ttl") Timer ttl,
            @StateId("buffer") BagState<InputT> buffer,
            @Timestamp Instant timestamp,
            OutputReceiver<OutputT> out,
            BoundedWindow window) {
        flush(out);
        settingElementTTLTimer(buffer, ttl, element); // abstract
        initialLoad(buffer, element, ttl); // abstract

        if (!alreadySentOrLoaded(buffer, element, ttl)) // abstract
            try {
                final UUID uuid = UUID.randomUUID();
                addIdempotentElementInBuffer(buffer, element);// abstract, WATCH OUT: potential race conditions
                final FutureT future = processElement(element);
                futures.put(uuid, handleOutput(future, element, buffer, uuid, timestamp, window));
            } catch (Exception e) {
                LOG.error("Failed to process element", e);
                throw e;
            }
        else
            outputAlreadySentOrLoaded(element, out); // abstract
    }
```

Thus, the notifications can be loaded in the "State" as initialLoad or if they have not been sent.
The Time To Live(TTL) can be set up as desired in the implementations or in _settingElementTTLTimer_.

An implementation of this class is shown
here: [StateAsyncParDoWithHttpHandler](http/StateAsyncParDoWithHttpHandler.scala).

It must be stated, that SCIO provides some options to deal with similar "caching" scenarios, please refer
to: https://spotify.github.io/scio/releases/migrations/v0.8.0-Migration-Guide.html#async-dofns, as _BaseAsyncLookupDoFn_
has a type parameter for some cache implementation, plugging in whatever cache supplier you want, e.g. a
com.google.common.cache.Cache, having it for handling TTL. Although, a concern here would be the scalability, therefore,
DataFlow Vertical autoscaling feature might come into place, tackling ingestion peaks.

This SCIO caching capability along with a workaround for adding something similar to
this [StateAsyncParDoWithHttpHandler](#StateAsyncParDoWithHttpHandler) using a S & T pattern, has been discussed as
potential future
enhancement here: https://github.com/spotify/scio/issues/5055.

### HTTPClient

"Bring your HTTP Client", like akka or zio here: [clients](http/clients). Implement it
as [AbstractHttpClient](http/clients/AbstractHttpClient.scala) and you can include it as **httpClient**:
```
  lazy val httpClient = {
    mediationConfig.mediation.httpClientType match {
      case "akka" => new AkkaHttpClient
      case "zio"  => new ZioHttpClient
      case "yourHttpClient"  => new YourHttpClient
    }
  }
```

within the [StateAsyncParDoWithHttpHandler](http/StateAsyncParDoWithHttpHandler.scala)

#### AkkaHttpClient

Information to implement an Akka HTTP Client can be
found https://doc.akka.io/docs/akka-http/current/client-side/index.html

This is the current implementation: [AkkaHttpClient](http/clients/akka/AkkaHttpClient.scala)

### ZIORetry

Some retry mechanism has been implemented using ZIO retry for Scala: https://zio.dev/reference/schedule/retrying/, so
that, we can avoid Dead Letter Queuing or Retry topic patterns. Retry is achieved at process level as shown below:

```
  def sendPushWithRetryZio(
    record: InputBer
  )(implicit zioRuntime: Runtime[Any]): StateAndTimerType.FutureKVOutputBerAndHttpResponse = {
    import zio._
    lazy val futureRetriableBer = ZIO
      .attempt {
        getResource.sendPushWithFutureResponse(newEventRecordWithRetryIncrement(record))
      }
      .retry(Schedule.fixed(BACKOFF_SECONDS.second) && Schedule.recurs(MAX_RETRIES))
      .onError(cause =>
        ZIO.succeed(
          log.error(s"[exhausted_notification=${record.getEvent.getTransactionId}] Retried error:${cause}", cause)
        )
      )

    Unsafe.unsafe { implicit unsafe =>
      zioRuntime.unsafe.run(futureRetriableBer).getOrThrowFiberFailure()
    }
  }
```

## Testing

### StateAsyncParDoWithHttpHandler

- Understanding the main purpose and design of the State and Timer pattern, keeping the idempotent state for
  _MyEventRecord_ notifications, before sending them as _MyHttpRequest_, just run in
  Intellij: [MediationServiceSpec_1_OK_2_DUPLICATES](../../../../../.run/MediationServiceSpec.1 OK and 2 SENT_OR_DUPLICATED HTTP_RESPONSE should exist in the same stream.run.xml)

> _HINT 1_: it uses the DirectRunner in your local machine (do not worry about not having a GCP project).

> _HINT 2_: PubSub emulator in Java needs this
> workaround https://cloud.google.com/pubsub/docs/emulator#pubsub-emulator-java. I have not got it sorted (yet). In the
> meantime we can just get by with TestStream.

## StressTests

Some "Stress testing" has been undertaken with Mock data sets (with different idempotent_key), peaking more than +200K
notifications / min with the current AkkaHttpClient config, forcing the application to scale up and keeping some million
of
notifications saved as "ttl" State:

```
    akka {
      max-open-requests = 20000
      max-open-connection = 20000
      initial-timeout = 30
      completion-timeout = 60
      buffer = 20000
      throttle-requests = 1000
      throttle-per-second = 1
      throttle-burst = 1000
    }
```

Bearing in mind that the same Mock data sets have been used in all scenarios, it is worth noting (with all e2 family GCE
machine type):

- e2-highcpu-4: scaling was needed (maxWorkers=3).
- e2-standard-4: we got good balance between scalability, latency and cost. Scaling was not needed.
- e2-highcpu-4: we got some OutOfMemory issues (restarting a worker), it did handle the load but with the worst
  performance.

## Background

Here it comes some inspiration and guidance for this project, coming mainly for previous Beam Summits:

- intro to State and Timer (from Beam Summit 2019) https://www.youtube.com/watch?v=Q_v5Zsjuuzg
- great intro to SCIO Beam (from Beam Summit 2021) https://github.com/iht/scio-scala-beam-summit
- State and Timer (from Beam Summit 2023)
  https://beamsummit.org/sessions/2023/too-big-to-fail-a-beam-pattern-for-enriching-a-stream-using-state-and-timers/
- Unbreakable & Supercharged Beam Apps with Scala + ZIO (from Beam Summit
  2023)https://beamsummit.org/sessions/2023/unbreakable-supercharged-beam-apps-with-scala-zio/

by these Beam Summit contributors:

- Kenneth Knowles: https://beamsummit.org/speakers/kenneth-knowles/
- Reza Rokni: https://2021.beamsummit.org/speakers/reza-rokni/
- Israel Herraiz: https://github.com/iht
- Tobias Kaymak: https://beamsummit.org/speakers/tobias-kaymak/
- Aris Vlasakakis: https://beamsummit.org/speakers/aris-vlasakakis/
- Sahil Khandwala: https://beamsummit.org/speakers/sahil-khandwala/



