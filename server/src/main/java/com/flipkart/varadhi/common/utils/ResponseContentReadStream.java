package com.flipkart.varadhi.common.utils;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;
import lombok.extern.slf4j.Slf4j;

/*
 * This can be used when stream response is required for an API.
 * Current uses is used in Http Chunked response (primarily Dlq.GetMessages() response).
 * ResponseHandler reads the content from this stream, while application can write
 * content to this stream via (send()).
 */
@Slf4j
public class ResponseContentReadStream implements ReadStream<Buffer> {

    private boolean paused = false;
    private boolean ended = false;

    private io.vertx.core.Handler<Buffer> dataHandler;
    private io.vertx.core.Handler<Void> endHandler;
    private io.vertx.core.Handler<Throwable> exceptionHandler;


    public ResponseContentReadStream() {
    }

    @Override
    public ReadStream<Buffer> handler(io.vertx.core.Handler<Buffer> handler) {
        log.info("Stream handler");
        this.dataHandler = handler;
        return this;
    }

    @Override
    public ReadStream<Buffer> pause() {
        log.info("Stream paused");
        this.paused = true;
        return this;
    }

    @Override
    public ReadStream<Buffer> resume() {
        log.info("Stream resumed");
        this.paused = false;
        return this;
    }

    @Override
    public ReadStream<Buffer> fetch(long l) {
        return null;
    }

    @Override
    public ReadStream<Buffer> endHandler(io.vertx.core.Handler<Void> handler) {
        log.info("Stream endhandler");
        this.endHandler = handler;
        return this;
    }

    @Override
    public ReadStream<Buffer> exceptionHandler(io.vertx.core.Handler<Throwable> handler) {
        log.info("Stream exception");
        this.exceptionHandler = handler;
        return this;
    }

    public void send(byte[] data) {
        Buffer buffer = Buffer.buffer(data);
        send(buffer);
    }

    public void send(Buffer buffer) {
        // TODO::handle paused and ended.
        if (!paused && !ended) {
            if (dataHandler != null) {
                dataHandler.handle(buffer);
                log.info("Data sent");
            }
        }
    }

    public void end() {
        endStream();
    }

    public void end(Throwable throwable) {
        ended = true;
        if (exceptionHandler != null) {
            exceptionHandler.handle(throwable);
        }
    }

    private void endStream() {
        log.info("Stream closed");
        ended = true;
        if (endHandler != null) {
            endHandler.handle(null);
        }
    }
}
