package com.jake.messaging.server.broker;

import com.jake.messaging.server.message.Message;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class Sender {

    private final KafkaTemplate<String, Message> kafkaTemplate;

    public void send(String topic, Message message) {
        kafkaTemplate.send(topic, message);
    }
}
