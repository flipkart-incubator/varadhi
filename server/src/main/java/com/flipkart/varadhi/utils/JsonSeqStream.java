package com.flipkart.varadhi.utils;

import com.flipkart.varadhi.common.utils.JsonMapper;
import io.vertx.core.buffer.Buffer;

/*
 * Can be used to send Chunked http response along with Content-type application/json-seq
 * This stream will append a RECORD_SEPARATOR to each object which is written to it.
 * Concurrent calls to send() are handled by underlying data handlers i.e. data from
 * concurrent calls to send() will not be corrupted/mixed.
 */
public class JsonSeqStream extends ResponseContentReadStream {
    private static final char RECORD_SEPARATOR = '\u001E';

    public <T> void send(T data) {
        byte[] dataBytes = JsonMapper.jsonSerializeAsBytes(data);
        Buffer buffer = Buffer.buffer(dataBytes);
        buffer.appendInt(RECORD_SEPARATOR);
        super.send(buffer);
    }
}
