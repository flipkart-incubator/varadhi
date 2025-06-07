package com.flipkart.varadhi.pulsar.entities;

import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.flipkart.varadhi.spi.services.DummyProducer;
import com.flipkart.varadhi.entities.JsonMapper;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PulsarOffsetTest {
    @BeforeEach
    public void foo() {
        JsonMapper.getMapper().registerSubtypes(new NamedType(PulsarOffset.class, "PulsarOffset"));
    }

    @Test
    public void defaultOffsetWorks() {
        PulsarOffset p1 = new PulsarOffset(MessageId.earliest);
        PulsarOffset p2 = new PulsarOffset(MessageId.latest);
        Assertions.assertEquals(0, p1.compareTo(new PulsarOffset(MessageId.earliest)));
        Assertions.assertEquals(0, p2.compareTo(new PulsarOffset(MessageId.latest)));
        Assertions.assertEquals(0, p2.compareTo(p2));
        Assertions.assertEquals(-1, p1.compareTo(p2));
        Assertions.assertEquals(1, p2.compareTo(p1));
    }

    @Test
    public void testNonDefaultOffsets() {
        PulsarOffset p1 = new PulsarOffset(MessageId.earliest);
        PulsarOffset p2 = new PulsarOffset(MessageId.latest);
        PulsarOffset p3 = new PulsarOffset(new MessageIdImpl(10, 10, 10));
        Assertions.assertEquals(0, p3.compareTo(new PulsarOffset(new MessageIdImpl(10, 10, 10))));
        Assertions.assertEquals(1, p3.compareTo(p1));
        Assertions.assertEquals(-1, p3.compareTo(p2));
        Assertions.assertEquals(1, p3.compareTo(new PulsarOffset(new MessageIdImpl(10, 9, 10))));
        Assertions.assertEquals(1, p3.compareTo(new PulsarOffset(new MessageIdImpl(9, 10, 10))));
        Assertions.assertEquals(1, p3.compareTo(new PulsarOffset(new MessageIdImpl(10, 10, 9))));
        Assertions.assertEquals(-1, p3.compareTo(new PulsarOffset(new MessageIdImpl(11, 10, 10))));
        Assertions.assertEquals(-1, p3.compareTo(new PulsarOffset(new MessageIdImpl(10, 11, 10))));
        Assertions.assertEquals(-1, p3.compareTo(new PulsarOffset(new MessageIdImpl(10, 10, 11))));
    }

    @Test
    public void NullNotComparable() {
        PulsarOffset p1 = new PulsarOffset(MessageId.earliest);
        IllegalArgumentException ae = Assertions.assertThrows(IllegalArgumentException.class, () -> p1.compareTo(null));
        Assertions.assertEquals("Can not compare null Offset.", ae.getMessage());
    }

    @Test
    public void CanNotCompareIncompatibleTypes() {
        PulsarOffset p1 = new PulsarOffset(MessageId.earliest);
        DummyProducer.DummyOffset dp1 = new DummyProducer.DummyOffset(1);
        IllegalArgumentException ae = Assertions.assertThrows(IllegalArgumentException.class, () -> p1.compareTo(dp1));
        Assertions.assertEquals(
            String.format(
                "Can not compare different Offset types. Expected Offset is %s, given  %s.",
                p1.getClass().getName(),
                dp1.getClass().getName()
            ),
            ae.getMessage()
        );
    }

    @Test
    public void testToString() {
        MessageId id1 = new MessageIdImpl(10, 9, 1);
        PulsarOffset p1 = new PulsarOffset(id1);
        Assertions.assertEquals("mId:" + id1, p1.toString());
    }
}
