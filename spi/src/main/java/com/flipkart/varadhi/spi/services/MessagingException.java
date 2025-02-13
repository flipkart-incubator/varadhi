package com.flipkart.varadhi.spi.services;

import lombok.experimental.StandardException;

/*
 * Varadhi abstracts interactions with underlying messaging tech stack.
 * Any messaging tech specific exception, which Varadhi's abstraction layer needs to
 * handle/understand should be wrapped and thrown accordingly.
 * MessagingException servers as the base class for such cases.
 */
@StandardException
public class MessagingException extends RuntimeException {
}
