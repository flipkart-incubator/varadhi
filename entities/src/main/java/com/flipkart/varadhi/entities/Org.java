package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.ValidateVaradhiResource;
import lombok.EqualsAndHashCode;
import lombok.Value;

import static com.flipkart.varadhi.Constants.INITIAL_VERSION;

@Value
@EqualsAndHashCode(callSuper = true)
@ValidateVaradhiResource(message = "Invalid Org name. Check naming constraints.")
public class Org extends VaradhiResource {

    public Org(
            String name,
            int version
    ) {
        super(name, version);
    }

    public Org cloneForCreate() {
        return new Org(getName(), INITIAL_VERSION);
    }
}
