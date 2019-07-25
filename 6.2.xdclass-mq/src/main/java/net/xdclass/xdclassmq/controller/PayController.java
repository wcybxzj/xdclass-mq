package net.xdclass.xdclassmq.controller;

import net.xdclass.xdclassmq.jms.JmsConfig;
import net.xdclass.xdclassmq.jms.PayProducer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;

@RestController
public class PayController {

    @Autowired
    private PayProducer payProducer;


    @RequestMapping("/api1")
    public Object callback(String text) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {

        Message message = new Message(JmsConfig.TOPIC,"taga", ("hello xdclass rocketmq = "+text).getBytes() );

        SendResult sendResult = payProducer.getProducer().send(message);
        System.out.println(sendResult);

        return new HashMap<>();
    }

}
