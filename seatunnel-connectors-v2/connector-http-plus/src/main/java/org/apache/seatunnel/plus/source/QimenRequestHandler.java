package org.apache.seatunnel.plus.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.plus.config.HttpParameter;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;

import static org.apache.seatunnel.plus.source.ApiTest.CHARSET_UTF8;
import static org.apache.seatunnel.plus.source.ApiTest.buildQuery;
import static org.apache.seatunnel.plus.source.ApiTest.signTopRequest;

public class QimenRequestHandler extends AbstractRequestHandler {
    private static final String SIGN_METHOD_HMAC = "hmac";

    @Override
    public void request(Config pluginConfig, HttpParameter httpParameter) {
        try {
            // httpParameter.getParams().put("page_index", "1");
            httpParameter.setHeaders(new HashMap<>());
            httpParameter
                    .getHeaders()
                    .put(
                            "Content-Type",
                            "application/x-www-form-urlencoded;charset=" + CHARSET_UTF8);
            httpParameter
                    .getParams()
                    .put(
                            "timestamp",
                            LocalDateTime.now(ZoneId.of("+8"))
                                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            if (httpParameter.getParams() != null
                    && httpParameter.getParams().containsKey("sign")) {
                httpParameter.getParams().remove("sign");
            }
            if (Integer.valueOf(httpParameter.getParams().get("page_index")) == 3) {
                httpParameter.getParams().remove("page_index");
            }
            httpParameter
                    .getParams()
                    .put(
                            "sign",
                            signTopRequest(
                                    httpParameter.getParams(),
                                    "015e246d33f2479453d0cb9f588df501",
                                    SIGN_METHOD_HMAC));
            httpParameter.setBody(buildQuery(httpParameter.getParams(), CHARSET_UTF8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
