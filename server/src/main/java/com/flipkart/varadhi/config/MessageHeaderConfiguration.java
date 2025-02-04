package com.flipkart.varadhi.config;

import com.flipkart.varadhi.utils.ValidateHeaderPrefix;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;


@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@ValidateHeaderPrefix
public class MessageHeaderConfiguration {
    @NotNull
    private List<String> allowedPrefix;

    // Callback codes header
    @NotNull
    private String callbackCodes;

    // Consumer timeout header (indefinite message consumer timeout)
    @NotNull
    private String requestTimeout;

    // HTTP related headers
    @NotNull
    private String replyToHttpUriHeader;

    @NotNull
    private String replyToHttpMethodHeader;

    @NotNull
    private String replyToHeader;

    @NotNull
    private String httpUriHeader;

    @NotNull
    private String httpMethodHeader;

    @NotNull
    private String httpContentType;

    // Group ID & Msg ID header used to correlate messages
    @NotNull
    private String groupIdHeader;

    @NotNull
    private String msgIdHeader;

}
