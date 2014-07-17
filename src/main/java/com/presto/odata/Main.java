package com.presto.odata;

import org.odata4j.producer.jdbc.Jdbc;
import org.odata4j.producer.jdbc.JdbcModelToMetadata;
import org.odata4j.producer.resources.DefaultODataProducerProvider;

public class Main {

    public static void main(String[] args) throws NoSuchFieldException, IllegalAccessException {

        PrestoJDBCProducer producer = PrestoJDBCProducer.newBuilder()
                .jdbc(new Jdbc("com.facebook.presto.jdbc.PrestoDriver", "jdbc:presto://localhost:8080/hive/", "test", null))
//                .insert(ProducerCommandContext.class, new LoggingCommand())
                .register(JdbcModelToMetadata.class, new PrestoModelToMetadataMapper())
                .build();

        DefaultODataProducerProvider.setInstance(producer);
        new ODataServerFactory().hostODataServer("http://localhost:8888/PrestoODataService.svc/");
    }

}
