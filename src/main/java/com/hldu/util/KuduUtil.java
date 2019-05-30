package com.hldu.util;

import org.apache.kudu.client.KuduClient;

public class KuduUtil {

    private static final String MASTERADDRESS = "10.11.2.1:7051";

    public static KuduClient getConf(){
        KuduClient conf = new KuduClient.KuduClientBuilder(MASTERADDRESS).defaultSocketReadTimeoutMs(6000).build();
        return conf;
    }
}
