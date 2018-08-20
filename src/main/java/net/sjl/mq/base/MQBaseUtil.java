package net.sjl.mq.base;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @Description: RabbitMQ使用基类
 *
 * @Author:shijialei
 * @Version:1.0
 * @Date:2018/8/20
 */
public class MQBaseUtil {

    private String host;

    private int port;

    private String userName;

    private String password;

    public MQBaseUtil(String host, int port, String userName, String password) {
        this.host = host;
        this.port = port;
        this.userName = userName;
        this.password = password;
    }

    public Connection initConnection() {
        Connection connection = null;
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(host);
        connectionFactory.setPort(port);
        connectionFactory.setUsername(userName);
        connectionFactory.setPassword(password);
        try {
            connection = connectionFactory.newConnection();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }

}
