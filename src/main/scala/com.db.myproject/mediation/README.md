# mediation-service

## Introduction

- Medium Post:

## Design
- based on KV State & Timer pattern: https://beam.apache.org/blog/timely-processing/
- leverages https://spotify.github.io/scio/releases/migrations/v0.8.0-Migration-Guide.html#async-dofns for attaching a
  https client for reaching an endpoint (e.g: https://jsonplaceholder.typicode.com/guide/) and send the MyEventRecord,
  also known as _BusinessEventRecord_ (BER, if
  you come across this acronym sorry)
- applying State (as idempotent_key in BagState ) and Timer, avoiding duplicates with same idempotent_key as long Timer
  is not flushed.

## Modelling

1. Using Avro [MyEventRecord](src/main/scala/com/db/myproject/mediation/avro/MyEventRecord.java) as Avro (new/historical
   notifications)
2. for HTTPResponse and HTTPRequest [notification.model](src/main/scala/com/db/myproject/mediation/notification/model)

<br />

## Flow

1. Reading **historical_notifications** from Google Cloud Storage (GCS)
2. Reading **new_notifications** from PubSub
3. Treating **historical_notifications** , unionAll with Bounded and Unbounded data stalls the process and GroupByKey
   (GBK) when applying state (for State And Timer).
   Thus, _SideInput approach is taken_ (as long as TTL is applied) for discarding duplicates from **new_notifications**
   against **historical_notifications**.
4. KO inValidBers as toxic in GCS
5. Saving attempted BERs in KV as State and release them when Timer expires (TTL): avoiding duplicates (race conditions
   are mitigated as distinctByKey is previously applied)
6. Saving NOTIFICATION_RESPONSE in PubSub (e.g: for analytics as external table, as **historical_notifications**)
   <br />

## Testing
### unit testing

- Understanding the State and Timer pattern keeping the idempotent state, before sending the HttpRequest,
  just [MediationServiceSpec_1_OK_2_DUPLICATES](.run/MediationServiceSpec.1 OK and 2 SENT_OR_DUPLICATED HTTP_RESPONSE should exist in the same stream.run.xml)
  <br />

## Other insights

- BER stress test data sets (+200K) has been undertaking with the current akka config
  <br />

