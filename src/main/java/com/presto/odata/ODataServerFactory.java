package com.presto.odata;

import org.odata4j.core.Throwables;
import org.odata4j.jersey.producer.resources.ODataApplication;
import org.odata4j.jersey.producer.server.ODataJerseyServer;
import org.odata4j.producer.resources.RootApplication;
import org.odata4j.producer.server.ODataServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ODataServerFactory {

    public void hostODataServer(String baseUri) {
        ODataServer server = null;
        try {
            server = startODataServer(baseUri);

            System.out.println("Press any key to exit");
            new BufferedReader(new InputStreamReader(System.in)).readLine();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        } finally {
            if (server != null)
                server.stop();
        }
    }

    public ODataServer startODataServer(String baseUri) {
        return createODataServer(baseUri).start();
    }

    public ODataServer createODataServer(String baseUri) {
        return new ODataJerseyServer(baseUri, ODataApplication.class, RootApplication.class);
    }
}
