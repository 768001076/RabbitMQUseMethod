package net.sjl.mq.provider;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;
import net.sjl.mq.base.MQBaseUtil;

import java.util.Random;

/**
 * @Description: MQ生产者
 *
 * @Author:shijialei
 * @Version:1.0
 * @Date:2018/8/20
 */
public class MQProvider {

    public static MQBaseUtil mqBaseUtil = new MQBaseUtil("127.0.0.1", 5672, "mimajiushitest", "test");

    public static String queue = "MQUseTestMethod";

    private final static String EXCHANGE_NAME = "ex_log";

    private final static String[] sers = {"1","2","3"};

    public static void main(String[] args) {
        sendTopicExchangesMessage();
    }

    /**
     * @Description: 最简单的发送方式
     *
     * @Auther: shijialei
     * @Date: 2018/8/20 14:50
     * @Version: 1.0
     * @Param: []
     * @Return: void
     * @Throws: []
     */
    public static void sendSimpleMessage() {
        // 初始化链接
        Connection connection = mqBaseUtil.initConnection();
        try {
            // 创建通道
            Channel channel = connection.createChannel();
            // 绑定队列
            channel.queueDeclare(queue, false, false, false, null);
            // 消息
            String message = "3411179153";
            // 发送消息
            channel.basicPublish("", queue, null, message.getBytes());
            System.out.println(" [x] Sent '" + message + "'");
            //关闭频道和连接
            channel.close();
            connection.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * @Description: 工作队列
     *
     * @Auther: shijialei
     * @Date: 2018/8/20 14:51
     * @Version: 1.0
     * @Param: []
     * @Return: void
     * @Throws: []
     */
    public static void sendWorkQueueMessage() {
        // 初始化链接
        Connection connection = mqBaseUtil.initConnection();
        try {
            // 创建通道
            Channel channel = connection.createChannel();
            // 是否持久化队列
            boolean durable = true;
            // 绑定队列
            channel.queueDeclare(queue, durable, false, false, null);
            for (int i = 0; i < 50; i++) {
                // 消息
                String message = "message:" + i;
                // 发送消息
                channel.basicPublish("", queue, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
                System.out.println(" [x] Sent '" + message + "'");
            }
            //关闭频道和连接
            channel.close();
            connection.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @Description: 简单转发
     *
     * @Auther: shijialei
     * @Date: 2018/8/20 14:52
     * @Version: 1.0
     * @Param: []
     * @Return: void
     * @Throws: []
     */
    public static void sendSimpleExchangesMessage() {
        // 初始化链接
        Connection connection = mqBaseUtil.initConnection();
        try {
            // 创建通道
            Channel channel = connection.createChannel();
            // 绑定转发器
            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

            for (int i = 0; i < 10; i++) {
                // 消息
                String message = "message:" + i;
                // 发送消息
                channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes());
                System.out.println(" [x] Sent '" + message + "'");
            }
            //关闭频道和连接
            channel.close();
            connection.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @Description: 路由转发
     *
     * @Auther: shijialei
     * @Date: 2018/8/20 14:54
     * @Version: 1.0
     * @Param: []
     * @Return: void
     * @Throws: []
     */
    public static void sendRouteExchangesMessage() {
        // 初始化链接
        Connection connection = mqBaseUtil.initConnection();
        try {
            // 创建通道
            Channel channel = connection.createChannel();
            // 绑定转发器
            channel.exchangeDeclare(EXCHANGE_NAME, "direct");
            for (int i = 0; i < 30; i++) {
                String routeKey = getRouteKey();
                // 消息
                String message = "message:" + i;
                // 发送消息
                channel.basicPublish(EXCHANGE_NAME, routeKey, null, message.getBytes());
                System.out.println(" [x] Sent -->" + routeKey + ": '" + message + "'");
            }
            //关闭频道和连接
            channel.close();
            connection.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @Description: 主题转发
     *
     * @Auther: shijialei
     * @Date: 2018/8/20 14:54
     * @Version: 1.0
     * @Param: []
     * @Return: void
     * @Throws: []
     */
    public static void sendTopicExchangesMessage() {
        // 初始化链接
        Connection connection = mqBaseUtil.initConnection();
        try {
            // 创建通道
            Channel channel = connection.createChannel();
            // 绑定转发器
            channel.exchangeDeclare(EXCHANGE_NAME, "topic");
            String[] topickey = {"com.ff","com.cc","gnh.ff","thy.dd"};
            for (String topic:topickey) {
                // 消息
                String message = "message:" + topic;
                // 发送消息
                channel.basicPublish(EXCHANGE_NAME, topic, null, message.getBytes());
                System.out.println(" [x] Sent -->" + topic + ": '" + message + "'");
            }
            //关闭频道和连接
            channel.close();
            connection.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String getRouteKey() {
        int i = new Random().nextInt(3);
        return sers[i];
    }

}
