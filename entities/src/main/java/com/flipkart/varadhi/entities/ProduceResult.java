package com.flipkart.varadhi.entities;


import lombok.Getter;

@Getter
public class ProduceResult {
    private String messageId;
    private long produceTimestamp;
    private ProducerResult producerResult;

    public ProduceResult(Message message, ProducerResult producerResult) {
        this.messageId = message.getMessageId();
        //TODO::fix produceTimestamp
//        this.produceTimestamp = message.;
        this.producerResult = producerResult;
    }

    public ProduceRestResponse getProduceRestResponse() {
        return new ProduceRestResponse(this.messageId);
    }
}
