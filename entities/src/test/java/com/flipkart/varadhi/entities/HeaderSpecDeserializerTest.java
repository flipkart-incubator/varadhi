package com.flipkart.varadhi.entities;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HeaderSpecDeserializerTest {

    @Test
    void parseRequiredBy_mandatoryAlias_matchesBoth() {
        assertEquals(RequiredBy.Both, HeaderSpecDeserializer.parseRequiredBy("mandatoryHeaderRequiredForProduce"));
        assertEquals(RequiredBy.Both, HeaderSpecDeserializer.parseRequiredBy("MANDATORYHEADERREQUIREDFORPRODUCE"));
    }

    @Test
    void parseRequiredBy_enumNames() {
        assertEquals(RequiredBy.Queue, HeaderSpecDeserializer.parseRequiredBy("Queue"));
        assertEquals(RequiredBy.Both, HeaderSpecDeserializer.parseRequiredBy("Both"));
    }

    @Test
    void parseRequiredBy_empty_defaultsToMandatoryProduce() {
        assertEquals(RequiredBy.mandatoryHeaderRequiredForProduce(), HeaderSpecDeserializer.parseRequiredBy(""));
    }

    @Test
    void parseRequiredBy_invalid_throws() {
        assertThrows(IllegalArgumentException.class, () -> HeaderSpecDeserializer.parseRequiredBy("notAnEnum"));
        assertThrows(IllegalArgumentException.class, () -> HeaderSpecDeserializer.parseRequiredBy("Callback"));
        assertThrows(IllegalArgumentException.class, () -> HeaderSpecDeserializer.parseRequiredBy("Subscription"));
    }
}
