package org.apache.ignite.internal.client.impl;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class ReproducerRestClient extends GridCommonAbstractTest {
    private static final String CLIENT_NAME = "client";
    public static final String CACHE_NAME = "RestCache";

    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setConsistentId(name);

        //cfg.setActiveOnStart(false);
        //cfg.setClientMode(CLIENT_NAME.equals(name));

        cfg.setCacheConfiguration(new CacheConfiguration(CACHE_NAME));

        /*cfg.setDiscoverySpi(new TcpDiscoverySpi()
            .setIpFinder(new TcpDiscoveryMulticastIpFinder()
                .setAddresses(Collections.singleton("127.0.0.1:47500..47510"))));

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration().
                setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)));*/

        return cfg;
    }

    @Test
    public void restRequestTest() throws Exception {
        /*final IgniteEx server = startGrid(1);
        server.cluster().active(true);*/

        ResponseHandler<String> handler = new BasicResponseHandler();

        HttpClient httpClient = HttpClientBuilder.create().build();
        try {
            HttpGet getRequest = new HttpGet("http://localhost:8080/ignite?cmd=version");
            //getRequest.addHeader("accept", "application/xml");
            HttpResponse response = httpClient.execute(getRequest);

            System.out.println(handler.handleResponse(response));

            final HttpPut putRequest = new HttpPut();
            putRequest.setURI(new URI("http://localhost:8080/ignite?cmd=put&key=newKey&val=newValue&cacheName=" + CACHE_NAME));
            //cmd=put&key=newKey&val=newValue&cacheName=partionedCache&destId=8daab5ea-af83-4d91-99b6-77ed2ca06647
            response = httpClient.execute(putRequest);

            System.out.println(handler.handleResponse(response));
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }
}
