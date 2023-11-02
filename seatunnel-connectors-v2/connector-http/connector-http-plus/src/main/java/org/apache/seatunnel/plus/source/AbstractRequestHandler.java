package org.apache.seatunnel.plus.source;

import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

public abstract class AbstractRequestHandler {

  public void before(Config pluginConfig){

  }

  public void after(Config pluginConfig){

  }

   abstract public void request(Config pluginConfig, HttpParameter httpParameter);
}
