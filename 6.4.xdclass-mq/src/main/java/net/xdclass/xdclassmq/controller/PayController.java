package net.xdclass.xdclassmq.controller;

import net.xdclass.xdclassmq.jms.JmsConfig;
import net.xdclass.xdclassmq.jms.PayProducer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;

@RestController
public class PayController {

    @Autowired
    private PayProducer payProducer;

    //同步/异步/oneway 投递任务
    @RequestMapping("/api1")
    public Object callback(String text) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {

        Message message = new Message(JmsConfig.TOPIC,"taga" ,"66881" , ("hello xdclass rocketmq = "+text).getBytes() );

        payProducer.getProducer().send(message, 1000);

        //payProducer.getProducer().sendOneway(message);

        /*
        payProducer.getProducer().send(message, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {

                try {
                    Thread.sleep(5000);
                    System.out.println("sleep finish!");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }


                System.out.printf("发送结果=%s, msg=%s ", sendResult.getSendStatus(), sendResult.toString());

            }

            @Override
            public void onException(Throwable e) {
                e.printStackTrace();
                //补偿机制，根据业务情况进行使用，看是否进行重试
            }

        });
        */

        return new HashMap<>();
    }

    //延迟消费
    @RequestMapping("/api2")
    public Object callback2(String text) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {

        Message message = new Message(JmsConfig.TOPIC,"taga" ,"66881" , ("hello xdclass rocketmq = "+text).getBytes() );

        //xxx是级别，1表示配置里面的第一个级别，2表示第二个级别
        //"1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h"
        message.setDelayTimeLevel(2);
        payProducer.getProducer().send(message, 1000);
        return new HashMap<>();
    }


    //同步投递消息到特定topic的特定queue
    @RequestMapping("/api3")
    public Object callback3(String text) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {

        Message message = new Message(JmsConfig.TOPIC,"taga" ,"66881" , ("hello xdclass rocketmq = "+text).getBytes() );

        SendResult sendResult = payProducer.getProducer().send(message, new MessageQueueSelector() {
            @Override
            public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                int queueNum = Integer.parseInt(arg.toString());
                return mqs.get(queueNum);
            }
        },0);//0是说投递到第一个队列去

        System.out.printf("发送结果=%s, msg=%s ", sendResult.getSendStatus(), sendResult.toString());

        return new HashMap<>();
    }

    //异步投递消息到特定topic的特定queue
    @RequestMapping("/api4")
    public Object callback4(String text) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {

        Message message = new Message(JmsConfig.TOPIC,"taga" ,"66881" , ("hello xdclass rocketmq = "+text).getBytes() );

        payProducer.getProducer().send(message, new MessageQueueSelector() {
            @Override
            public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                int queueNum = Integer.parseInt(arg.toString());
                return mqs.get(queueNum);
            }
        }, 3, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.printf("发送结果=%s, msg=%s ", sendResult.getSendStatus(), sendResult.toString());
            }

            @Override
            public void onException(Throwable e) {
                e.printStackTrace();
            }
        });

        return new HashMap<>();
    }



}
