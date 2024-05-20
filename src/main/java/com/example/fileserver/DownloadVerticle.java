package com.example.fileserver;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;


public class DownloadVerticle extends AbstractVerticle {
 private static final Logger log = LogManager.getLogger(RestAPIVerticle.class);

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    startPromise.complete();
    log.info("Deployed {}!", DownloadVerticle.class.getName());
    JsonObject address = config().getJsonObject("addresses");


        vertx.eventBus().consumer(address.getString("download"),hdl ->{
          String fileName = hdl.body().toString();
          String filePath = config().getString("location") + fileName;

          String bucketName = config().getString("S3bucketName");
          String keyName = fileName;

          System.out.format("Downloading %s from S3 bucket %s...\n", keyName, bucketName);
          final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.DEFAULT_REGION).build();
          try {
            S3Object s3Object = s3.getObject(bucketName, keyName);
            S3ObjectInputStream s3is = s3Object.getObjectContent();
            File outputFile = new File(filePath);

            // Create parent directories if they do not exist
            outputFile.getParentFile().mkdirs();

            try (FileOutputStream fos = new FileOutputStream(outputFile)) {
              byte[] readBuf = new byte[1024];
              int readLen;
              while ((readLen = s3is.read(readBuf)) > 0) {
                fos.write(readBuf, 0, readLen);
              }
            }

            byte[] fileData = Files.readAllBytes(outputFile.toPath());

            // Send the file data back as a reply to the request
            hdl.reply(fileData);

            log.info("File downloaded successfully");
            s3is.close();
          } catch (AmazonServiceException e) {
            log.error("Amazon exception: {}", e.getErrorMessage());
            hdl.fail(404, "File not found");
          } catch (FileNotFoundException e) {
            log.error("File not found exception: {}", e.getMessage());
            hdl.fail(404, "File not found");
          } catch (IOException e) {
            log.error("IO exception: {}", e.getMessage());
            hdl.fail(404, "File not found");
          }

        });
      }


}
