package com.example.fileserver;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class UploadVerticle extends AbstractVerticle {
 private static final Logger log = LogManager.getLogger(UploadVerticle.class);
//  private static final String UPLOADS_DIRECTORY = "C:\\uploads\\";
//private static final String UPLOADS_DIRECTORY = "/home/shashika/uploads/";
//private static final String UPLOADS_DIRECTORY = "/home/ec2-user/uploads/";
  @Override
  public void start() {
    JsonObject address = config().getJsonObject("addresses");



        log.info("Deployed {}!", UploadVerticle.class.getName());
        vertx.eventBus().consumer(address.getString("upload"), message -> {
          Buffer buffer = (Buffer) message.body(); // Get the Buffer object
          String filename = message.headers().get("filename");
//          System.out.println(buffer.getName());
          log.info("file name {}",filename);

          byte[] fileData = buffer.getBytes();
         Bucket b = createBucket(config().getString("S3bucketName"));
          uploadFileToBucket(b.getName(),filename, fileData,config().getString("location"));
        });


  }

//  private void saveFile(String filename, byte[] fileData,String location) {
//    try {
//      Path filePath = Paths.get(location + filename);
//      Path fp = Files.write(filePath, fileData);
//      log.info("File saved {}",fp.toString());
//      log.info("File saved successfully at : {} ", filePath);
//    } catch (IOException e) {
//      e.printStackTrace();
//      log.error("Error occurred");
//    }
//  }
  public static Bucket createBucket(String bucket_name) {
    final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.DEFAULT_REGION).build();
    Bucket b = null;
    if (s3.doesBucketExistV2(bucket_name)) {
      log.info("Bucket {} already exists", bucket_name);
      b = getBucket(bucket_name);
      System.out.println(b.getName());
    } else {
      try {
        b = s3.createBucket(bucket_name);
      } catch (AmazonS3Exception e) {
        System.err.println(e.getErrorMessage());
      }
    }
    return b;
  }
  public static Bucket getBucket(String bucket_name) {
    final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.DEFAULT_REGION).build();
    Bucket named_bucket = null;
    List<Bucket> buckets = s3.listBuckets();
    for (Bucket b : buckets) {
      if (b.getName().equals(bucket_name)) {
        named_bucket = b;
      }
    }
    return named_bucket;
  }

  public static void uploadFileToBucket(String bucketName,String filename, byte[] fileData,String location){

    Path filePath = Paths.get(location + filename);
    try{
      Path fp = Files.write(filePath, fileData);
      log.info("File is saved in local machine");
    }
    catch (IOException e){
      log.error(e.getMessage());
    }

//    File f = new File()
    String bucket_name = bucketName;
//    String file_path = args[1];
    String key_name =filename;

    System.out.format("Uploading %s to S3 bucket %s...\n", filePath, bucket_name);
    final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.DEFAULT_REGION).build();
    try {
      s3.putObject(bucket_name, key_name, new File(filePath.toString()));
        try{
          Files.delete(filePath);
        }
        catch (IOException e){
          log.error("couldn't deleted stored file in the local {}",e.getMessage());
        }
    } catch (AmazonServiceException e) {
     log.error(e.getErrorMessage());
//      System.exit(1);
    }
    log.info("file uploaded success fully!");
  }


}
