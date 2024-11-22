/*
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
package org.apache.polaris.service.ratelimiter;

import static org.apache.polaris.core.monitor.PolarisMetricRegistry.SUFFIX_ERROR;
import static org.apache.polaris.core.monitor.PolarisMetricRegistry.TAG_RESP_CODE;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micrometer.core.instrument.Tag;
import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.polaris.service.test.PolarisIntegrationTestHelper;
import org.apache.polaris.service.test.TestMetricsUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.threeten.extra.MutableClock;

/** Main integration tests for rate limiting */
@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestProfile(RateLimiterFilterTest.Profile.class)
public class RateLimiterFilterTest {

  private static final long REQUESTS_PER_SECOND = 5;
  private static final Duration WINDOW = Duration.ofSeconds(10);

  // FIXME these constants come from TimedApplicationEventListener
  /**
   * Each API will increment a common counter (SINGLETON_METRIC_NAME) but have its API name tagged
   * (TAG_API_NAME).
   */
  public static final String SINGLETON_METRIC_NAME = "polaris.api";

  public static final String TAG_API_NAME = "api_name";

  @Inject PolarisIntegrationTestHelper testHelper;

  private static MutableClock clock = MockRealmTokenBucketRateLimiter.CLOCK;

  @BeforeAll
  public void setUp(TestInfo testInfo) {
    QuarkusMock.installMockForType(
        new MockRealmTokenBucketRateLimiter(REQUESTS_PER_SECOND, WINDOW), RateLimiter.class);
    testHelper.setUp(testInfo);
  }

  @AfterAll
  public void tearDown() {
    testHelper.tearDown();
  }

  @BeforeEach
  @AfterEach
  public void resetRateLimiter() {
    clock.add(WINDOW.multipliedBy(2)); // Clear any counters from before/after this test
  }

  @Test
  public void testRateLimiter() {
    Consumer<Response.Status> requestAsserter =
        TestUtil.constructRequestAsserter(testHelper, testHelper.realm);

    for (int i = 0; i < REQUESTS_PER_SECOND * WINDOW.toSeconds(); i++) {
      requestAsserter.accept(Response.Status.OK);
    }
    requestAsserter.accept(Response.Status.TOO_MANY_REQUESTS);

    // Ensure that a different realm identifier gets a separate limit
    Consumer<Response.Status> requestAsserter2 =
        TestUtil.constructRequestAsserter(testHelper, testHelper + "2");
    requestAsserter2.accept(Response.Status.OK);
  }

  @Test
  public void testMetricsAreEmittedWhenRateLimiting() {
    Consumer<Response.Status> requestAsserter =
        TestUtil.constructRequestAsserter(testHelper, testHelper.realm);

    for (int i = 0; i < REQUESTS_PER_SECOND * WINDOW.toSeconds(); i++) {
      requestAsserter.accept(Response.Status.OK);
    }
    requestAsserter.accept(Response.Status.TOO_MANY_REQUESTS);

    assertTrue(
        TestMetricsUtil.getTotalCounter(
                testHelper,
                SINGLETON_METRIC_NAME + SUFFIX_ERROR,
                List.of(
                    Tag.of(TAG_API_NAME, "polaris.principal-roles.listPrincipalRoles"),
                    Tag.of(
                        TAG_RESP_CODE,
                        String.valueOf(Response.Status.TOO_MANY_REQUESTS.getStatusCode()))))
            > 0);
  }

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "polaris.rate-limiter.type", "realm-token-bucket",
          "polaris.rate-limiter.realm-token-bucket.requests-per-second",
              String.valueOf(REQUESTS_PER_SECOND),
          "polaris.rate-limiter.realm-token-bucket.window", WINDOW.toString());
    }
  }
}
