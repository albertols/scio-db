package com.db.myproject.mediation.http.state;

import com.spotify.scio.transforms.DoFnWithResource;
import com.spotify.scio.transforms.FutureHandlers;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

/**
 * A {@link DoFn} that handles asynchronous requests to an external service with State and Timer.
 *
 * Based on: https://github.com/spotify/scio/blob/main/scio-core/src/main/java/com/spotify/scio/transforms/BaseAsyncDoFn.java
 */
public abstract class StateBaseAsyncDoFn<InputT, OutputT, ResourceT, FutureT>
        extends DoFnWithResource<InputT, OutputT, ResourceT>
        implements FutureHandlers.Base<FutureT, OutputT> {
    private static final Logger LOG = LoggerFactory.getLogger(StateBaseAsyncDoFn.class);
    private final ConcurrentMap<UUID, FutureT> futures = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<Result> results = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
    @TimerId("ttl")
    private final TimerSpec ttlSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @StateId("buffer")
    private final StateSpec<BagState<InputT>> bufferedEvents = StateSpecs.bag();

    /**
     * Process an element asynchronously.
     */
    public abstract FutureT processElement(InputT input);

    @DoFn.StartBundle
    public void startBundle(StartBundleContext context) {
        futures.clear();
        results.clear();
        errors.clear();
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext context) {
        if (!futures.isEmpty()) {
            try {
                LOG.info("Flushing Futures");
                waitForFutures(futures.values());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.error("Failed to process futures", e);
                new RuntimeException("Failed to process futures", e);
            } catch (ExecutionException e) {
                LOG.error("Failed to process futures", e);
                new RuntimeException("Failed to process futures", e);
            }
        }
        // STEP 4.3)
        flush(context);
    }


    @ProcessElement
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
                // STEP 4.1)
                futures.put(uuid, handleOutput(future, element, buffer, uuid, timestamp, window));
            } catch (Exception e) {
                LOG.error("Failed to process element", e);
                throw e;
            }
        else
            outputAlreadySentOrLoaded(element, out); // abstract
    }

    protected abstract void outputAlreadySentOrLoaded(InputT element, OutputReceiver<OutputT> out);

    protected abstract void initialLoad(@StateId("buffer") BagState<InputT> buffer, InputT element, @TimerId("ttl") Timer ttl);

    protected abstract boolean alreadySentOrLoaded(@StateId("buffer") BagState<InputT> buffer, InputT element, @TimerId("ttl") Timer ttl);

    protected abstract void settingElementTTLTimer(@StateId("buffer") BagState<InputT> buffer, @TimerId("ttl") Timer ttl, InputT element);

    private FutureT handleOutput(FutureT future, InputT input, @StateId("buffer") BagState<InputT> buffer, UUID key, Instant timestamp, BoundedWindow window) {
        return addCallback(
                future,
                output -> {
                    results.add(new Result(output, key, timestamp, window));
                    return null;
                },
                throwable -> {
                    errors.add(throwable);
                    return null;
                });
    }

    protected abstract void addIdempotentElementInBuffer(BagState<InputT> buffer, InputT input);

    private void flush(OutputReceiver<OutputT> outputReceiver) {
        if (!errors.isEmpty()) {
            RuntimeException e = new RuntimeException("Failed to process futures");
            Throwable t = errors.poll();
            while (t != null) {
                e.addSuppressed(t);
                t = errors.poll();
            }
            LOG.error("Flushing out before processing element: ", e);
            //throw e; // REMOVED for avoiding stalling
        }
        Result r = results.poll();
        while (r != null) {
            outputReceiver.output(r.output);
            futures.remove(r.futureUuid);
            r = results.poll();
        }
    }

    private void flush(FinishBundleContext c) {
        if (!errors.isEmpty()) {
            RuntimeException e = new RuntimeException("Failed to process futures");
            Throwable t = errors.poll();
            while (t != null) {
                e.addSuppressed(t);
                t = errors.poll();
            }
            LOG.error("Flushing out at the end of @FinishBundle: ", e);
            // throw e; // REMOVED for avoiding stalling
        }
        Result r = results.poll();
        while (r != null) {
            c.output(r.output, r.timestamp, r.window);
            futures.remove(r.futureUuid);
            r = results.poll();
        }
    }

    @OnTimer("ttl")
    public void onTtl(
            OnTimerContext c,
            @StateId("buffer") BagState<InputT> buffer) {
        long count = java.util.stream.StreamSupport.stream(
                        java.util.Spliterators.spliteratorUnknownSize(
                                buffer.read().iterator(), java.util.Spliterator.ORDERED), false)
                .count();
        LOG.info("****  Deleting, buffer_size={} **** ", count);
        buffer.clear();
    }

    private class Result {
        private final OutputT output;
        private final UUID futureUuid;
        private final Instant timestamp;
        private final BoundedWindow window;

        Result(OutputT output, UUID futureUuid, Instant timestamp, BoundedWindow window) {
            this.output = output;
            this.timestamp = timestamp;
            this.futureUuid = futureUuid;
            this.window = window;
        }
    }
}





