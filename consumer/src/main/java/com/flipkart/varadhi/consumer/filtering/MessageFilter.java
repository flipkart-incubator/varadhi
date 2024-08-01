package com.flipkart.varadhi.consumer.filtering;

import com.flipkart.varadhi.entities.Message;

import java.util.function.Predicate;

public class MessageFilter implements Predicate<Message> {
    @Override
    public boolean test(Message message) {
        return true;
    }
}
