package com.swarchitecture.kafka.producer.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class TextProducer implements Producer {
    
    private static final Logger logger = LoggerFactory.getLogger(TextProducer.class);

    @Autowired
    private KafkaTemplate<String, String> textKafkaTemplate;

    @Value("${topic}")
    private String topic;

    private String message;

    public TextProducer setMessage(String message) {
        this.message = message;
        return this;
    }

    @Override
    public void send() {
        textKafkaTemplate.send(topic, message);
    }
}
