package com.flipkart.varadhi.cluster;

import org.jboss.byteman.agent.submit.ScriptText;
import org.jboss.byteman.agent.submit.Submit;

import java.util.List;
import java.util.Properties;

/**
 * Control Byteman on a Varadhi pod started with {@code withByteman(true)}.
 *
 * <p>Default boot script arms faults via system properties (reliable with already-loaded classes).
 * {@link #installScript(String)} remains available for dynamic rules when retransform works.
 */
public final class BytemanControl {

    public static final String FAIL_HEALTH_PROP = "org.jboss.byteman.failHealth";
    public static final String FAIL_PRODUCE_NOT_FOUND_PROP = "org.jboss.byteman.failProduceNotFound";

    private final Submit submit;

    public BytemanControl(String host, int listenerPort) {
        this.submit = new Submit(host, listenerPort);
    }

    /** Arm/disarm the boot-time health-check fault rule. */
    public void setFailHealth(boolean fail) throws Exception {
        setFlag(FAIL_HEALTH_PROP, fail);
    }

    /**
     * Arm/disarm produce-path {@code ResourceNotFoundException} injection
     * ({@code ProduceHandlers.produce} → HTTP 404).
     */
    public void setFailProduceNotFound(boolean fail) throws Exception {
        setFlag(FAIL_PRODUCE_NOT_FOUND_PROP, fail);
    }

    private void setFlag(String property, boolean value) throws Exception {
        Properties props = new Properties();
        props.setProperty(property, Boolean.toString(value));
        submit.setSystemProperties(props);
    }

    /** Install a single inline rule script (one or more RULE…ENDRULE blocks). */
    public void installScript(String script) throws Exception {
        String result = submit.addScripts(List.of(new ScriptText("inline", script)));
        if (result != null && (result.contains("ERROR") || result.contains("FAILED") || result.contains("Exception"))) {
            throw new IllegalStateException("Byteman rejected rules: " + result);
        }
    }

    public void clearRules() throws Exception {
        submit.deleteAllRules();
    }

    public String listRules() throws Exception {
        return submit.listAllRules();
    }

    public String agentVersion() throws Exception {
        return submit.getAgentVersion();
    }
}
