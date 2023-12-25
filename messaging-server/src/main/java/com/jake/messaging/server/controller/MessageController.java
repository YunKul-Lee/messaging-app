package com.jake.messaging.server.controller;

import com.jake.messaging.server.broker.Sender;
import com.jake.messaging.server.message.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessageSendingOperations;
import org.springframework.stereotype.Controller;

@Controller
@RequiredArgsConstructor
@Slf4j
public class MessageController {

    private final Sender sender;
    private final SimpMessageSendingOperations messageTemplate;

    @MessageMapping("/chat.send-message")
    public void sendMessage(@Payload Message chatMessage, SimpMessageHeaderAccessor headerAccessor) {
        chatMessage.setSessionId(headerAccessor.getSessionId());
        sender.send("messaging", chatMessage);
        log.info("Sending message to /topic/public: {}", chatMessage);
        messageTemplate.convertAndSend("/topic/public", chatMessage);
        log.info("Message sent to /topic/public: {}", chatMessage);
    }

    @MessageMapping("/chat.add-user")
    public Message addUser(@Payload Message chatMessage, SimpMessageHeaderAccessor headerAccessor) {
        if(headerAccessor.getSessionAttributes() != null) {
            headerAccessor.getSessionAttributes().put("username", chatMessage.getSender());
        }

        return chatMessage;
    }

}
