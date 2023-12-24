package com.jake.messaging.server.broker;

import com.jake.messaging.server.message.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessageSendingOperations;
import org.springframework.messaging.simp.user.SimpSession;
import org.springframework.messaging.simp.user.SimpUser;
import org.springframework.messaging.simp.user.SimpUserRegistry;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class Receiver {

    private final SimpMessageSendingOperations messageSendingOperations;
    private final SimpUserRegistry userRegistry;

    @KafkaListener(topics = "messaging", groupId = "chat")
    public void consume(Message chatMessage) {
        log.info("Received message from kafka : {}", chatMessage);

        for(SimpUser user : userRegistry.getUsers()) {
            for(SimpSession session : user.getSessions()) {
                if(!session.getId().equals(chatMessage.getSessionId())) {
                    messageSendingOperations.convertAndSendToUser(session.getId(), "/topic/public", chatMessage);
                }
            }
        }
    }
}
