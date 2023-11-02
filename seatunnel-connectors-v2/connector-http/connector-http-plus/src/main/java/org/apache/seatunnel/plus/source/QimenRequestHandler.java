package org.apache.seatunnel.plus.source;

import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

import static org.apache.seatunnel.plus.source.ApiTest.CHARSET_UTF8;
import static org.apache.seatunnel.plus.source.ApiTest.buildQuery;
import static org.apache.seatunnel.plus.source.ApiTest.signTopRequest;

public class QimenRequestHandler extends AbstractRequestHandler{
  private static final String SIGN_METHOD_HMAC="md5";
  @Override
  public void request(Config pluginConfig, HttpParameter httpParameter) {
    try {
      httpParameter.getParams().put("method", "jushuitan.order.list.query");
      httpParameter.getParams().put("app_key", "34289603");
      //params.put("session", sessionKey);
      DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      httpParameter.getParams().put("timestamp", df.format(new Date()));
      httpParameter.getParams().put("format", "json");
      httpParameter.getParams().put("v", "2.0");
      httpParameter.getParams().put("sign_method", "hmac");
      httpParameter.getParams().put("customer_id","11276573");
      httpParameter.getParams().put("target_app_key","23060081");
      httpParameter.getParams().put("start_time","2023-10-01");
      httpParameter.getParams().put("end_time","2023-10-07");
      httpParameter.getParams().put("page_size","100");
      // 业务参数
      //params.put("fields", "num_iid,title,nick,price,num");
      //params.put("num_iid", "123456789");
      // 签名参数
      httpParameter.getParams().put("sign", signTopRequest(httpParameter.getParams(), "015e246d33f2479453d0cb9f588df501", SIGN_METHOD_HMAC));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }


}
