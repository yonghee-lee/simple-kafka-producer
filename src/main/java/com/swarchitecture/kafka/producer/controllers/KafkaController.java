package com.swarchitecture.kafka.producer.controllers;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.swarchitecture.kafka.producer.services.FileProducer;
import com.swarchitecture.kafka.producer.services.TextProducer;

@RestController
public class KafkaController {


    @Autowired
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    @PostMapping("/publish/text")
    public ResponseEntity<String> sendMessage(
            @RequestParam(value = "topic") String topic,
            @RequestBody String message) {
        TextProducer textProducer = new TextProducer(topic, message);
        textProducer.send();
        return ResponseEntity.ok("Message sent successfully");
    }

    @PostMapping("/publish/file")
    public CompletableFuture<ResponseEntity<String>> uploadFile(@RequestParam("file") MultipartFile file,
                                                                 @RequestParam("topic") String topic,
                                                                 @RequestParam(value = "chunkSize", defaultValue = "1048576") int chunkSize) throws IOException {
        if (file.isEmpty()) {
            throw new FileNotFoundException("File is empty");
        }
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("Chunk size must be greater than zero");
        }

        String fileName = file.getOriginalFilename();
        byte[] fileContent = file.getBytes();

        // Create a new FileSender to send the file chunks
        FileProducer fileSender = new FileProducer(fileName, fileContent, topic, chunkSize, kafkaTemplate);
        fileSender.send();

        // Return a CompletableFuture that will be completed when all chunks have been sent
        return CompletableFuture.completedFuture(ResponseEntity.ok("File uploaded successfully"));
    }
}
