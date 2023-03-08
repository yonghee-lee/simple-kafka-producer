package com.swarchitecture.kafka.producer.controllers;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.swarchitecture.kafka.producer.services.FileProducer;
import com.swarchitecture.kafka.producer.services.TextProducer;

@RestController
@RequestMapping("/api")
public class KafkaController {


    @Autowired
    private TextProducer textProducer;

    @Autowired
    private FileProducer fileProducer;

    @PostMapping("/publish/text")
    public ResponseEntity<String> sendMessage(
            @RequestBody String message) {
        textProducer.setMessage(message).send();
        return ResponseEntity.ok("Message sent successfully");
    }

    @PostMapping("/publish/file")
    public ResponseEntity<String> sendFile(@RequestParam("path") String filePath) throws IOException {
        
        //CompletableFuture<ResponseEntity<String>> future = new CompletableFuture<>();
        fileProducer.getFileHandler(filePath).send();
        //future.complete(ResponseEntity.ok("File sending initiated"));
        return ResponseEntity.ok("File sending initiated.");
        //return future;
}
}
