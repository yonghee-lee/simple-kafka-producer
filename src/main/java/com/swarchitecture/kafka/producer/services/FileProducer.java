package com.swarchitecture.kafka.producer.services;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.swarchitecture.kafka.producer.components.FileHandler;
import com.swarchitecture.kafka.producer.components.HandlerFactory;

@Service
public class FileProducer implements Producer {
    
    private final Logger logger = LoggerFactory.getLogger(FileProducer.class);

    @Autowired
    private HandlerFactory handlerFactory;

    @Autowired
    private KafkaTemplate<String,byte[]> fileKafkaTemplate;

    @Value("${topic}")
    private String topic;

    private FileHandler fileHandler;

    public FileProducer getFileHandler(String filePath) {
        
        try {
            logger.info("File Path: {}", filePath);
            fileHandler = handlerFactory.createFileHandler(filePath);
        } catch (IOException e) {
            
            logger.error("Error: {}", e.getMessage());
        }
        return this;
    }

    public void send() {
        
        boolean isCompleted = false;

        while (!isCompleted) {
            
            Map<String,Object> chunkInfo = new HashMap<>();

            try {
                chunkInfo = fileHandler.getChunkInfo();
            } catch(IOException e) {
                logger.error("File handling error.");
            }

            isCompleted = (boolean)chunkInfo.get("isCompleted");

            String key = (String) chunkInfo.get("key");
            byte[] chunk = (byte[])chunkInfo.get("chunk");
            
            ListenableFuture<SendResult<String, byte[]>> kafkaFuture = fileKafkaTemplate.send(topic, key, chunk);

            kafkaFuture.addCallback(new ListenableFutureCallback<SendResult<String, byte[]>>() {
                @Override
                public void onSuccess(@Nullable SendResult<String, byte[]> result) {
                    if(result != null)
                        logger.info("Sent chunk {} to {} partition " ,key,result.getRecordMetadata().partition());
                
                }

                @Override
                public void onFailure(Throwable ex) {
                    logger.error("Failed to send chunk {}: {}",key,ex.getMessage());
                }
            });
    
        }

        // Complete the CompletableFuture once all chunks have been sent
        fileKafkaTemplate.flush();
    }
}
