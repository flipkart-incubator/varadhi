package com.flipkart.varadhi.auth;

import lombok.ToString;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@ToString
public class ResourceName {

    private static final Pattern variablePattern = Pattern.compile("\\{[a-zA-Z0-9._-]+}");

    private final List<Token> tokens = new ArrayList<>();

    private boolean hasVariables;

    private final int sizeEstimate;

    public ResourceName(String value) {
        sizeEstimate = value.length() * 2;

        // TODO: move this to static method
        Matcher matcher = variablePattern.matcher(value);

        int readIdx = 0;
        while (matcher.find()) {
            if (matcher.start() > 0) {
                tokens.add(new Token(value.substring(readIdx, matcher.start()), false));
            }
            tokens.add(new Token(value.substring(matcher.start() + 1, matcher.end() - 1), true));
            readIdx = matcher.end();
            hasVariables = true;
        }

        if (readIdx < value.length()) {
            tokens.add(new Token(value.substring(readIdx), false));
        }
    }

    public boolean hasVariables() {
        return hasVariables;
    }

    public String resolve(Function<String, String> env) {
        StringBuilder sb = new StringBuilder(sizeEstimate);
        for (Token t : tokens) {
            if (t.isVariable) {
                String resolvedValue = env.apply(t.token);
                if (resolvedValue == null) {
                    throw new IllegalStateException("could not resolve variable '" + t.token + "'");
                }
                sb.append(resolvedValue);
            } else {
                sb.append(t.token);
            }
        }
        return sb.toString();
    }

    record Token(String token, boolean isVariable) {
    }
}
