package com.swarchitecture.kafka.producer.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class TextProducer implements Producer {
    
    private static final Logger logger = LoggerFactory.getLogger(TextProducer.class);

    @Autowired
    private KafkaTemplate<String, String> textKafkaTemplate;

    private String topic;
    private String message;

    public TextProducer(String topic, String message) {
        this.topic = topic;
        this.message = message;
    }

    @Override
    public void send() {
        textKafkaTemplate.send(topic, message);
    }
}
