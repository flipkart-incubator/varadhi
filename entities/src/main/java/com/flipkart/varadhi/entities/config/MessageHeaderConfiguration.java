package com.flipkart.varadhi.entities.config;

import com.flipkart.varadhi.entities.Validatable;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import jakarta.validation.constraints.NotNull;
import lombok.*;

import java.lang.reflect.Method;
import java.util.List;


@Data
@Builder
@AllArgsConstructor
public class MessageHeaderConfiguration implements Validatable {
    @NotNull
    private final List<String> allowedPrefix;

    // Callback codes header
    @NotNull
    private final String callbackCodes;

    // Consumer timeout header (indefinite message consumer timeout)
    @NotNull
    private final String requestTimeout;

    // HTTP related headers
    @NotNull
    private final String replyToHttpUriHeader;

    @NotNull
    private final String replyToHttpMethodHeader;

    @NotNull
    private final String replyToHeader;

    @NotNull
    private final String httpUriHeader;

    @NotNull
    private final String httpMethodHeader;

    @NotNull
    private final String httpContentType;

    // Group ID & Msg ID header used to correlate messages
    @NotNull
    private final String groupIdHeader;

    @NotNull
    private final String msgIdHeader;

    @NotNull
    private final Integer headerValueSizeMax;

    @NotNull
    private final String produceTimestamp;

    @NotNull
    private final String produceRegion;

    @NotNull
    private final String produceIdentity;

    @NotNull
    private final Integer maxRequestSize;


    /**
     * We use reflection to dynamically invoke getter methods for all fields in the
     * `MessageHeaderConfiguration` class, allowing us to validate them without
     * explicitly coding checks for each field. This makes the validation process
     * scalable and easier to maintain, as any new fields with getters will be
     * automatically validated. The `allowedPrefix` field is skipped as it is handled separately.
     */
    @SneakyThrows
    @Override
    public void validate() {
        List<String> allowedPrefixList = getAllowedPrefix();
        for (String prefix : allowedPrefixList) {
            if (prefix.isEmpty() || prefix.isBlank()) {
                throw new IllegalArgumentException("Header prefix cannot be blank");
            }
        }

        for (Method method : MessageHeaderConfiguration.class.getDeclaredMethods()) {
            if (isGetterMethod(method)) {
                if ("getAllowedPrefix".equals(method.getName())) {
                    continue;
                }
                Object value = method.invoke(this);
                if (value instanceof String stringValue) {
                    if (!startsWithValidPrefix(allowedPrefixList, stringValue)) {
                        throw new IllegalArgumentException(
                                method.getName() + " does not have a valid header value : " + stringValue);
                    }
                }
            }
        }
    }

    private boolean isGetterMethod(Method method) {
        return method.getName().startsWith("get") && method.getReturnType() != void.class && method.getParameterCount() == 0;
    }

    private boolean startsWithValidPrefix(List<String> allowedPrefixes, String value) {
        for (String prefix : allowedPrefixes) {
            if (value.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    public static List<String> getRequiredHeaders(MessageHeaderConfiguration messageHeaderConfiguration) {
        return List.of(
                messageHeaderConfiguration.getMsgIdHeader(),
                messageHeaderConfiguration.getProduceTimestamp(),
                messageHeaderConfiguration.getProduceRegion(),
                messageHeaderConfiguration.getProduceIdentity()
        );
    }

    public static void ensureRequiredHeaders(MessageHeaderConfiguration messageHeaderConfiguration, Multimap<String, String> headers) {
        getRequiredHeaders(messageHeaderConfiguration).forEach(key -> {
            if (!headers.containsKey(key)) {
                throw new IllegalArgumentException(String.format("Missing required header %s", key));
            }
        });
    }

    public static Multimap<String, String> copyVaradhiHeaders(Multimap<String, String> headers, List<String> allowedPrefix) {
        Multimap<String, String> varadhiHeaders = ArrayListMultimap.create();
        headers.entries().forEach(entry -> {
            String key = entry.getKey();
            boolean validPrefix = allowedPrefix.stream().anyMatch(key::startsWith);
            if (validPrefix) {
                varadhiHeaders.put(key, entry.getValue());
            }
        });
        return varadhiHeaders;
    }

    public static MessageHeaderConfiguration fetchDummyHeaderConfiguration(){
        return new MessageHeaderConfiguration(
                List.of("X_","x_"),
                "X_CALLBACK_CODES",
                "X_REQUEST_TIMEOUT",
                "X_REPLY_TO_HTTP_URI",
                "X_REPLY_TO_HTTP_METHOD",
                "X_REPLY_TO",
                "X_HTTP_URI",
                "X_HTTP_METHOD",
                "X_CONTENT_TYPE",
                "X_GROUP_ID",
                "X_MESSAGE_ID",
                100,
                "X_PRODUCE_TIMESTAMP",
                "X_PRODUCE_REGION",
                "X_PRODUCE_IDENTITY",
                (5 * 1024 * 1024)
        );
    }

}
