package net.sjl.mq.consumer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.QueueingConsumer;
import net.sjl.mq.base.MQBaseUtil;

import java.io.IOException;
import java.util.Random;

/**
 * @Description: MQ消费者
 *
 * @Author:shijialei
 * @Version:1.0
 * @Date:2018/8/20
 */
public class MQConsumer {

    public static MQBaseUtil mqBaseUtil = new MQBaseUtil("127.0.0.1", 5672, "mimajiushitest", "test");

    public static String queue = "MQUseTestMethod";

    private final static String EXCHANGE_NAME = "ex_log";

    private final static String[] sers = {"1","2","3"};

    private final static String[] topics = {"com.*","*.ff","*.dd"};

    public static void main(String[] args) {
        getTopicExchangesMessage();
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
    public static void getSimpleMessage() {
        // 创建连接
        Connection connection = mqBaseUtil.initConnection();
        try {
            // 创建通道
            Channel channel = connection.createChannel();
            // 声明队列，主要为了防止消息接收者先运行此程序，队列还不存在时创建队列。
            channel.queueDeclare(queue, false, false, false, null);
            // 创建队列消费者
            QueueingConsumer queueingConsumer = new QueueingConsumer(channel);
            // 指定消费队列
            channel.basicConsume(queue, true, queueingConsumer);
            while (true)
            {
                //nextDelivery是一个阻塞方法（内部实现其实是阻塞队列的take方法）
                QueueingConsumer.Delivery delivery = queueingConsumer.nextDelivery();
                String message = new String(delivery.getBody());
                System.out.println(" [x] Received '" + message + "'");
            }
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
    public static void getWorkQueueMessage() {
        // 创建连接
        Connection connection = mqBaseUtil.initConnection();
        try {
            // 创建通道
            Channel channel = connection.createChannel();
            // 持久化
            boolean durable = true;
            // 声明队列，主要为了防止消息接收者先运行此程序，队列还不存在时创建队列。
            channel.queueDeclare(queue, durable, false, false, null);
            // 设置最大服务转发消息数量
            int prefetchCount = 1;
            channel.basicQos(prefetchCount);
            // 创建队列消费者
            QueueingConsumer queueingConsumer = new QueueingConsumer(channel);
            // 自动响应
            boolean ack = false;
            // 指定消费队列
            channel.basicConsume(queue, ack, queueingConsumer);
            while (true)
            {
                //nextDelivery是一个阻塞方法（内部实现其实是阻塞队列的take方法）
                QueueingConsumer.Delivery delivery = queueingConsumer.nextDelivery();
                String message = new String(delivery.getBody());
                work(message);
                //另外需要在每次处理完成一个消息后，手动发送一次应答。
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
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
    public static void getSimpleExchangesMessage() {
        // 创建连接
        Connection connection = mqBaseUtil.initConnection();
        try {
            // 创建通道
            Channel channel = connection.createChannel();
            // 转发器
            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
            // 创建一个非持久的、唯一的且自动删除的队列
            String queueName = channel.queueDeclare().getQueue();
            System.out.println(queueName);
            // 为转发器指定队列，设置binding
            channel.queueBind(queueName, EXCHANGE_NAME, "");
            // 创建队列消费者
            QueueingConsumer queueingConsumer = new QueueingConsumer(channel);
            // 自动响应
            boolean ack = false;
            // 指定消费队列
            channel.basicConsume(queueName, ack, queueingConsumer);
            while (true)
            {
                //nextDelivery是一个阻塞方法（内部实现其实是阻塞队列的take方法）
                QueueingConsumer.Delivery delivery = queueingConsumer.nextDelivery();
                String message = new String(delivery.getBody());
                work(message);
                //另外需要在每次处理完成一个消息后，手动发送一次应答。
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
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
    public static void getRouteExchangesMessage() {
        // 创建连接
        Connection connection = mqBaseUtil.initConnection();
        try {
            // 创建通道
            Channel channel = connection.createChannel();
            // 转发器
            channel.exchangeDeclare(EXCHANGE_NAME, "direct");
            // 创建一个非持久的、唯一的且自动删除的队列
            String queueName = channel.queueDeclare().getQueue();
            System.out.println(queueName);
            // routekey
            String routeKey = getRouteKey();
            System.out.println(routeKey);
            // 为转发器指定队列，设置binding
            channel.queueBind(queueName, EXCHANGE_NAME, routeKey);
            // 创建队列消费者
            QueueingConsumer queueingConsumer = new QueueingConsumer(channel);
            // 自动响应
            boolean ack = false;
            // 指定消费队列
            channel.basicConsume(queueName, ack, queueingConsumer);
            while (true)
            {
                //nextDelivery是一个阻塞方法（内部实现其实是阻塞队列的take方法）
                QueueingConsumer.Delivery delivery = queueingConsumer.nextDelivery();
                String message = new String(delivery.getBody());
                work(message);
                //另外需要在每次处理完成一个消息后，手动发送一次应答。
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
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
    public static void getTopicExchangesMessage() {
        // 创建连接
        Connection connection = mqBaseUtil.initConnection();
        try {
            // 创建通道
            Channel channel = connection.createChannel();
            // 转发器
            channel.exchangeDeclare(EXCHANGE_NAME, "topic");
            // 创建一个非持久的、唯一的且自动删除的队列
            String queueName = channel.queueDeclare().getQueue();
            System.out.println(queueName);
            // routekey
            String topicKey = getTopicKey();
            System.out.println(topicKey);
            // 为转发器指定队列，设置binding
            channel.queueBind(queueName, EXCHANGE_NAME, topicKey);
            // 创建队列消费者
            QueueingConsumer queueingConsumer = new QueueingConsumer(channel);
            // 自动响应
            boolean ack = false;
            // 指定消费队列
            channel.basicConsume(queueName, ack, queueingConsumer);
            while (true)
            {
                //nextDelivery是一个阻塞方法（内部实现其实是阻塞队列的take方法）
                QueueingConsumer.Delivery delivery = queueingConsumer.nextDelivery();
                String message = new String(delivery.getBody());
                work(message);
                //另外需要在每次处理完成一个消息后，手动发送一次应答。
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @Description: 工作
     *
     * @Auther: shijialei
     * @Date: 2018/8/20 15:18
     * @Version: 1.0
     * @Param: [message]
     * @Return: void
     * @Throws: []
     */
    public static void work(String message) {
        try {
            Thread.sleep(3000);
            System.out.println(" [x] Received '" + message + "'");
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static String getRouteKey() {
        int i = new Random().nextInt(3);
        return sers[i];
    }

    public static String getTopicKey() {
        int i = new Random().nextInt(3);
        return topics[i];
    }

}
