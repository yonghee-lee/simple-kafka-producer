package com.swarchitecture.kafka.producer.file;

import org.springframework.web.socket.BinaryMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;

@Component
public class FileWebSocketHandler implements WebSocketHandler {

    private final Logger logger = LoggerFactory.getLogger(FileWebSocketHandler.class);
    
    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    @Autowired
    public FileWebSocketHandler(KafkaTemplate<String, byte[]> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public Mono<Void> handle(WebSocketSession session) {
        return session.receive()
            .cast(BinaryMessage.class)
            .doOnNext(message -> this.sendToKafka(message))
            .then();
    }

    private void sendToKafka(BinaryMessage message) {
        try {
            ByteBuffer byteBuffer = message.getPayload();
            if (byteBuffer.remaining() <= 0) {
                return;
            }
            // Read the filename from the BinaryMessage
            String fileName = new String(byteBuffer.array(), 0, byteBuffer.remaining());
            // Remove any whitespace or newlines from the filename
            fileName = fileName.trim().replaceAll("\n|\r", "");
    
            // Extract the file data and send it to Kafka
            byte[] data = new byte[byteBuffer.remaining()];
            byteBuffer.get(data);
            kafkaTemplate.send("fileTopic", fileName, data);
        } catch (Exception e) {
            logger.error("Error sending to Kafka: ", e);
        }
    }
}
