package org.tus.servlet.upload.s3;

import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tus.servlet.upload.Config;
import org.tus.servlet.upload.Datastore;
import org.tus.servlet.upload.FileInfo;
import org.tus.servlet.upload.Locker;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.UploadPartRequest;

public class S3Store implements Datastore {/// Set up the request
    private static Request generateRequest(String reqBody) {
        Request request = new DefaultRequest(SERVICE_NAME);
        request.setContent(new ByteArrayInputStream(reqBody.getBytes()));
        request.setEndpoint(URI.create(ENDPOINT));
        request.setHttpMethod(HttpMethodName.POST);
        return request;
 }

 /// Perform Signature Version 4 signing
 private static void performSigningSteps(Request requestToSign) {
        AWS4Signer signer = new AWS4Signer();
        signer.setServiceName(SERVICE_NAME);
        signer.setRegionName(REGION);      

        // Get credentials
        // NOTE: *Never* hard-code credentials
        //       in source code
        AWSCredentials creds = new BasicAWSCredentials(accessKey, secretKey);

        // Sign request with supplied creds
        signer.sign(requestToSign, creds);
 }

 /// Send the request to the server
 private static void sendRequest(Request request) {
        ExecutionContext context = new ExecutionContext(true);

        ClientConfiguration clientConfiguration = new ClientConfiguration();
        AmazonHttpClient client = new AmazonHttpClient(clientConfiguration);

        MyHttpResponseHandler responseHandler = new MyHttpResponseHandler();
        MyErrorHandler errorHandler = new MyErrorHandler();

        Response response =
                      client.execute(request, responseHandler, errorHandler, context);
 }

 public static void main(String[] args) {
        // Generate the request
        Request request = generateRequest("{\"field\":\"fieldValue\"}");

        // Perform Signature Version 4 signing
        performSigningSteps(request);

        // Send the request to the server
        sendRequest(request);
 }

 public static class MyHttpResponseHandler implements HttpResponseHandler> {

        @Override
        public AmazonWebServiceResponse handle(
                         com.amazonaws.http.HttpResponse response) throws Exception {

                InputStream responseStream = response.getContent();
                String responseString = convertStreamToString(responseStream);
                System.out.println(responseString);

                AmazonWebServiceResponse awsResponse = new AmazonWebServiceResponse();
                return awsResponse;
        }

        @Override
        public boolean needsConnectionLeftOpen() {
                return false;
        }
 }

 public static class MyErrorHandler implements HttpResponseHandler {

        @Override
        public AmazonServiceException handle(
                         com.amazonaws.http.HttpResponse response) throws Exception {
                System.out.println("In exception handler!");

                AmazonServiceException ase = new AmazonServiceException("Fake service exception.");
                ase.setStatusCode(response.getStatusCode());
                ase.setErrorCode(response.getStatusText());
                return ase;
          }

        @Override
        public boolean needsConnectionLeftOpen() {
                return false;
          }
 }}
