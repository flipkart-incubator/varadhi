package com.flipkart.varadhi.entities;

public class StandardHeaders {
    public static String VARADHI_HEADER_PREFIX = "x_";
    public static String PRODUCE_TIMESTAMP = "x_restbus_produce_timestamp";
    public static String PRODUCE_REGION = "x_restbus_produce_region";
    public static String PRODUCE_IDENTITY = "x_restbus_produce_identity";
    public static String FORWARDED_FOR = "x-forwarded-for";
    public static String MESSAGE_ID = "x_restbus_message_id";
    public static String GROUP_ID = "x_restbus_group_id";
}
