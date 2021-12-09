/*
 * Copyright (C) 2021 jrhee17
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.jrhee17;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;

import com.linecorp.armeria.client.logging.LoggingClient;
import com.linecorp.armeria.client.retrofit2.ArmeriaRetrofit;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.RequestContext;
import com.linecorp.armeria.common.rxjava2.RequestContextAssembly;
import com.linecorp.armeria.common.util.SafeCloseable;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.logging.LoggingService;
import com.linecorp.armeria.testing.junit5.server.ServerExtension;

import io.reactivex.Single;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import retrofit2.adapter.rxjava2.RxJava2CallAdapterFactory;
import retrofit2.converter.jackson.JacksonConverterFactory;
import retrofit2.http.GET;

@Slf4j
class MixedContextTest {

    private static final long delayedMillis = 3000L;

    AsyncLoadingCache<String, Product> cache;

    @Value
    @NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
    @AllArgsConstructor
    public static class Product {
        String name;
    }

    interface ShopService {
        @GET("/")
        Single<Product> getProduct();
    }

    @RegisterExtension
    static ServerExtension server = new ServerExtension() {
        @Override
        protected void configure(ServerBuilder sb) {
            sb.service("/", (ctx, req) -> HttpResponse.delayed(
                    HttpResponse.ofJson(new Product("hello world")), Duration.ofMillis(delayedMillis)));
            sb.decorator(LoggingService.newDecorator());
        }
    };

    @BeforeAll
    static void beforeAll() {
        RequestContextAssembly.enable();
    }

    @AfterAll
    static void afterAll() {
        RequestContextAssembly.disable();
    }

    @BeforeEach
    void beforeEach() {
        final ShopService shopService = ArmeriaRetrofit.builder(server.httpUri())
                                                       .decorator(LoggingClient.newDecorator())
                                                       .addConverterFactory(JacksonConverterFactory.create())
                                                       .addCallAdapterFactory(
                                                               RxJava2CallAdapterFactory.createAsync())
                                                       .build().create(ShopService.class);

        cache = Caffeine.newBuilder()
                        .buildAsync((String k, Executor e) -> {
                            log.info("running...");
                            final CompletableFuture<Product> future = new CompletableFuture<>();
                            shopService.getProduct().subscribe(
                                    future::complete, future::completeExceptionally);
                            return future;
                        });
    }

    @Test
    void testMixedContext() {
        final ServiceRequestContext ctx1 = ServiceRequestContext.builder(
                HttpRequest.of(HttpMethod.GET, "/1")).build();
        final ServiceRequestContext ctx2 = ServiceRequestContext.builder(
                HttpRequest.of(HttpMethod.GET, "/2")).build();
        final CompletableFuture<Product> f1, f2;
        try (SafeCloseable ignored = ctx1.push()) {
            f1 = cache.get("a").thenApply(product -> {
                log.info("1: {}", RequestContext.current().request().path());
                return product;
            });
        }
        try (SafeCloseable ignored = ctx2.push()) {
            f2 = cache.get("a").thenApply(product -> {
                log.info("2: {}", RequestContext.current().request().path());
                return product;
            });
        }

        try (SafeCloseable ignored = ctx2.push()) {
            assertThat(f2.join().getName()).isEqualTo("hello world");
        }
        try (SafeCloseable ignored = ctx1.push()) {
            assertThat(f1.join().getName()).isEqualTo("hello world");
        }
        log.info("done");
    }
}
