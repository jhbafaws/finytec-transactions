package com.kafka.finytec.transactions;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import com.kafka.finytec.transactions.models.FinytecTransaction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.List;

@SpringBootApplication
@EnableScheduling
public class FinytecTransactionsApplication {

    private static final Logger log = LoggerFactory.getLogger(FinytecTransactionsApplication.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired(required = true)
    private ObjectMapper mapper;

    @Autowired
    private RestHighLevelClient client;



    @KafkaListener(topics = "finytec-transactions", groupId = "finytecGroup", containerFactory = "kafkaListenerContainerFactory")
    public void listen(List<ConsumerRecord<String, String>> messages) throws JsonProcessingException , JsonMappingException {
        for (ConsumerRecord<String, String> message : messages) {
            //          FinytecTransaction transaction =  mapper.readValue(message.value(), FinytecTransaction.class );
//            log.info("Partition = {} Offset = {} key = {} Message = {}",
//                    message.partition(), message.offset(), message.key(), message.value());

            IndexRequest indexRequest = buildIndexRequest(String.format("%s-%s-%s", message.partition(), message.key(), message.offset()), message.value());

            client.indexAsync(indexRequest, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
                @Override
                public void onResponse(IndexResponse indexResponse) {
                    log.debug("Success Request");
                }

                @Override
                public void onFailure(Exception e) {

                    log.error("Error string message  {}", e.getMessage());
                }
            });

        }
    }

    private IndexRequest buildIndexRequest(String key, String value) {
        IndexRequest request = new IndexRequest("finytec-transactions");

        request.id(key);
        request.source(value, XContentType.JSON);
        return request;
    }

    @Scheduled(fixedRate = 15000)
    public void sendMessages() throws JsonProcessingException {
        Faker faker = new Faker();
        for (int i = 0; i < 10000; i++) {
            FinytecTransaction transaction = new FinytecTransaction();

            transaction.setName(faker.name().name());
            transaction.setLastName(faker.name().lastName());
            transaction.setUserName(faker.name().username());
            transaction.setAmount(faker.number().randomDouble(3, 0, 18000));

            kafkaTemplate.send("finytec-transactions", transaction.getUserName(), mapper.writeValueAsString(transaction));
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(FinytecTransactionsApplication.class, args);
    }

//    @Override
//    public void run(String... args) throws Exception {
//
//        IndexRequest indexRequest = new IndexRequest("finytec-transactions");
//        indexRequest.id("44");
//        indexRequest.source("{\"nombre\":\"Sammie\"}", XContentType.JSON);
//
//        IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
//
//        log.info("response id = {}", response.getId());
//    }
}
