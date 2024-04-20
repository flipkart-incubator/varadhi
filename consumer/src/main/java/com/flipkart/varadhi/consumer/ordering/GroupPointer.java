package com.flipkart.varadhi.consumer.ordering;

public class GroupPointer {
    QueueGroupPointer main;
    QueueGroupPointer[] retry;
    QueueGroupPointer deadLetter;
}
