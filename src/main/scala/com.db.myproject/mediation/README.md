# mediation-service

## Introduction

- replacing:
    - old microservices

## Design

- based on KV State & Timer pattern: https://beam.apache.org/blog/timely-processing/
- leverages https://spotify.github.io/scio/releases/migrations/v0.8.0-Migration-Guide.html#async-dofns for attaching a
  https client for reaching NHUB and send the MyEventRecord (BER)

## Flow

1. Reading **historical_bers** from GCS
2. Reading **new_bers** from PubSub
3. Treating **historical_bers** , unionAll with Bounded and Unbounded data stalls the process and GroupByKey (GBK), so
   _SideInput approach is taken_ (as long as TTL is applied)
4. KO inValidBers as toxic in GCS
5. Saving attempted BERs in KV as State and release them when Timer expires (TTL): avoiding duplicates (race conditions
   are mitigated as distinctByKey is previously applied)
6. Saving NHUB_RESPONSE in PubSub (for analytics as external table, as **historical_bers**)
   <br />

## Testing

### mocks

- BER stress test data sets (+200K)
- Quique DEBIT with null (not sent), sent and
- all under .run, e.g [BERAvroPubSubMockQuiqueNull](.run/BERAvroPubSubMockQuiqueNull.run.xml)
  <br />

### unit testing

- check [MediationServiceSpec](src/test/scala/pwcclakees/mediation/MediationServiceSpec.scala)
  <br />



