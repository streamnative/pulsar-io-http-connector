/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pulsar.io.http;

import lombok.extern.slf4j.Slf4j;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.reactor.IOReactorConfig;
import org.apache.hc.core5.util.Timeout;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

import java.util.Map;

@Slf4j
public class HttpSink implements Sink<byte[]> {

    private CloseableHttpAsyncClient client;

    private String scheme;

    private String host;

    private int port;

    private String uri;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        HttpSinkConfig httpSinkConfig = HttpSinkConfig.load(config);
        final IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
                .setSoTimeout(Timeout.ofNanoseconds(httpSinkConfig.getTimeoutNanoSeconds()))
                .build();
        client = HttpAsyncClients.custom()
                .setIOReactorConfig(ioReactorConfig)
                .build();
        client.start();
        scheme = httpSinkConfig.getScheme();
        host = httpSinkConfig.getHttpHost();
        port = httpSinkConfig.getHttpPort();
        uri = httpSinkConfig.getHttpUri();
    }

    @Override
    public void write(Record<byte[]> record) throws Exception {
        if (!record.getMessage().isPresent()) {
            record.ack();
            return;
        }
        Message<byte[]> message = record.getMessage().get();
        final SimpleHttpRequest request = SimpleRequestBuilder.get()
                .setHttpHost(new HttpHost(scheme, host, port))
                .setPath(uri)
                .build();
        client.execute(request, new FutureCallback<SimpleHttpResponse>() {
            @Override
            public void completed(SimpleHttpResponse simpleHttpResponse) {
                if (log.isDebugEnabled()) {
                    log.debug("send dis success message id is {}", message.getMessageId());
                }
                record.ack();
            }

            @Override
            public void failed(Exception e) {
                log.error("send to http server error, error is {}", e.getMessage());
                record.fail();
            }

            @Override
            public void cancelled() {
                log.error("send to http server error");
                record.fail();
            }
        });
    }

    @Override
    public void close() throws Exception {
        client.close();
    }
}
