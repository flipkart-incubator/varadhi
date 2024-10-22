package com.flipkart.varadhi.utils;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AsyncProducerReadStream implements ReadStream<Buffer> {

    private boolean paused = false;
    private boolean ended = false;

    private io.vertx.core.Handler<Buffer> dataHandler;
    private io.vertx.core.Handler<Void> endHandler;
    private io.vertx.core.Handler<Throwable> exceptionHandler;



    public AsyncProducerReadStream() {
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

    public void send(String data) {
        if (!paused && !ended) {
            if (dataHandler != null) {
                Buffer buffer = Buffer.buffer(data);
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
