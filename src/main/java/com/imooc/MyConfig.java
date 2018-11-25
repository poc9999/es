package com.imooc;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Company      : Shenzhen Greatonce Co Ltd
 * Created By   : Administrator
 * Created Date : 2018/11/24 20:14
 * Description  : es配置文件
 */
@Configuration
public class MyConfig {

    @Bean
    public TransportClient client() throws UnknownHostException {
        TransportAddress inetSocketAddress = new TransportAddress(
                InetAddress.getByName("localhost"),9300
        );

        Settings settings = Settings.builder()
                .put("cluster.name","caishen")
//                .put("")
                .build();
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(inetSocketAddress);
        return client;
    }
}
