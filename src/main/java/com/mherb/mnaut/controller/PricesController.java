package com.mherb.mnaut.controller;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.reactivex.Flowable;
import io.reactivex.Single;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

@Controller("/prices")
@Slf4j
public class PricesController {

    private final MongoClient mongoClient;

    public PricesController(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    @Get("/")
    public Flowable<Document> fetch() {
        var collection = getCollection();
        return Flowable.fromPublisher(collection.find());
    }

    @Post("/")
    public Single<InsertOneResult> insert(@Body ObjectNode json) {
        var collection = getCollection();
        final Document doc = Document.parse(json.toString());
        LOG.info("Insert {}", doc);
        return Single.fromPublisher(collection.insertOne(doc));
    }

    private MongoCollection<Document> getCollection() {
        return mongoClient.getDatabase("prices").getCollection("example");
    }
}
