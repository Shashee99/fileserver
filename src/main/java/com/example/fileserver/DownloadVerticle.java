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

          String bucket_name = config().getString("S3bucketName");
          String key_name = fileName;

          System.out.format("Downloading %s from S3 bucket %s...\n", key_name, bucket_name);
          final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.DEFAULT_REGION).build();
          try {
            S3Object o = s3.getObject(bucket_name, key_name);
            S3ObjectInputStream s3is = o.getObjectContent();
            File sf = new File(key_name);
            byte[] fileData = Files.readAllBytes(sf.toPath());

            // Send the file data back as a reply to the request
            hdl.reply(fileData);
//            System.out.println(sf.getName());
//            System.out.println(sf.length());
//            FileOutputStream fos = new FileOutputStream(new File(key_name));
//            System.out.println(fos.toString());

            // Send the file data back as a reply to the request
//            hdl.reply(fileData);
//            byte[] read_buf = new byte[1024];
//            int read_len = 0;
//            while ((read_len = s3is.read(read_buf)) > 0) {
//              fos.write(read_buf, 0, read_len);
//            }
            s3is.close();
//            fos.close();
          } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            hdl.fail(404,"file not found");
//            System.exit(1);
          } catch (FileNotFoundException e) {
            System.err.println(e.getMessage());
            hdl.fail(404,"file not found");
//            System.exit(1);
          } catch (IOException e) {
            System.err.println(e.getMessage());
            hdl.fail(404,"file not found");
//            System.exit(1);
          }
          log.info("file download successfully");

//          vertx.fileSystem().readFile(filePath, result -> {
//            if (result.succeeded()) {
//              byte[] fileData = result.result().getBytes();
//
//              // Send the file data back as a reply to the request
//              hdl.reply(fileData);
//            } else {
//              log.error("error occured");
//              hdl.fail(404, "File not found");
//            }
//          });

        });
      }


}
