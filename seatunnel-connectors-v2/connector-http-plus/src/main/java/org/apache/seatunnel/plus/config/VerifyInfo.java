package org.apache.seatunnel.plus.config;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@Setter
@Getter
@ToString
public class VerifyInfo implements Serializable {
    public String jsonVerifyExpression;
    private String jsonVerifyValue;
}
