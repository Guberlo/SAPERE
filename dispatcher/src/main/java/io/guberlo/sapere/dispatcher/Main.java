package io.guberlo.sapere.dispatcher;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

public class Main {

    // TODO: WRITE THIS MAIN
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        Dispatcher dispatcher = new Dispatcher("english", "/opt/app/config.yaml");
        dispatcher.start();
    }
}
