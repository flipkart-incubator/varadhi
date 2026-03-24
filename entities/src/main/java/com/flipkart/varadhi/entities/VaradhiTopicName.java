package com.flipkart.varadhi.entities;

import lombok.Value;

/**
 * Represents a fully-qualified Varadhi topic name, encapsulating the project
 * and local topic name that are stored together as "{project}.{topic}".
 */
@Value
public class VaradhiTopicName {

    String projectName;
    String topicName;

    /**
     * Constructs a VaradhiTopicName from explicit components.
     */
    public static VaradhiTopicName of(String projectName, String topicName) {
        return new VaradhiTopicName(projectName, topicName);
    }

    /**
     * Parses a fully-qualified topic name of the form "{project}.{topic}".
     *
     * @throws IllegalArgumentException if the format is invalid
     */
    public static VaradhiTopicName parse(String fqn) {
        String[] segments = fqn.split(Versioned.NAME_SEPARATOR_REGEX, -1);
        if (segments.length != 2 || segments[0].isBlank() || segments[1].isBlank()) {
            throw new IllegalArgumentException("Invalid topic name format, expected '{project}.{topic}', got: " + fqn);
        }
        return new VaradhiTopicName(segments[0], segments[1]);
    }

    /**
     * Returns the fully-qualified name as "{project}.{topic}".
     */
    public String toFqn() {
        return String.join(Versioned.NAME_SEPARATOR, projectName, topicName);
    }
}
