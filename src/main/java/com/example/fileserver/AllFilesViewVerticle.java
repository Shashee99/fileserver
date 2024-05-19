package com.example.fileserver;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class AllFilesViewVerticle extends AbstractVerticle {
 private static final Logger log = LogManager.getLogger(RestAPIVerticle.class);
  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    startPromise.complete();
    log.info("Deployed {}!", AllFilesViewVerticle.class.getName());
    JsonObject address = config().getJsonObject("addresses");
    vertx.eventBus().consumer(address.getString("allFiles"),message -> {
//      String filePath = "C:\\uploads";
      String filePath = config().getString("location");
      final JsonArray filenames = new JsonArray();

     log.info("Objects in S3 bucket {}", config().getString("S3bucketName"));
      final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.DEFAULT_REGION).build();
      ListObjectsV2Result result = s3.listObjectsV2(config().getString("S3bucketName"));
      List<S3ObjectSummary> objects = result.getObjectSummaries();
      objects.forEach( o -> filenames.add(o.getKey()));
      message.reply(filenames);
//      for (S3ObjectSummary os : objects) {
//        System.out.println("* " + os.getKey());
//        filesnames.forEach(filenames::add);
//      }
//      vertx.fileSystem().readDir(filePath, result -> {
//        if (result.succeeded()) {
//          List<String> filesnames = result.result();
//
////          HttpServerResponse response = context.response();
////          response.setStatusCode(200);
////          response.end(filenames.toBuffer());
//          System.out.println(filesnames.toString());
//          message.reply(filenames);


//        }
//        else {
//    log.error("error occured");
////          context.fail(404); // Not Found
//        }
//      });
    });
  }
}
