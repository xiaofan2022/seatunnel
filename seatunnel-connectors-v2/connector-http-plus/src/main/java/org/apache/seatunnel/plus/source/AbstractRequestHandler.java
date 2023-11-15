package org.apache.seatunnel.plus.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.plus.config.HttpParameter;

public abstract class AbstractRequestHandler {

    public void before(Config pluginConfig) {}

    public void after(Config pluginConfig) {}

    public abstract void request(Config pluginConfig, HttpParameter httpParameter);
}
