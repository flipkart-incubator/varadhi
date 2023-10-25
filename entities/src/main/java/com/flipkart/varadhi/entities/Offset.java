package com.flipkart.varadhi.entities;

import java.io.Serializable;

/*
 Offset of the message in the topic.
 Details are messaging stack specific.
 */

public interface Offset extends Comparable<Offset>, Serializable {

}
