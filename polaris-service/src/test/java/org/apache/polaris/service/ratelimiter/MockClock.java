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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import org.threeten.extra.MutableClock;

@Alternative
@ApplicationScoped
public class MockClock extends Clock {

  private final MutableClock delegate;

  public MockClock() {
    this.delegate = MutableClock.of(Instant.now(), ZoneOffset.UTC);
  }

  @Override
  public ZoneId getZone() {
    return delegate.getZone();
  }

  @Override
  public Clock withZone(ZoneId zone) {
    return delegate.withZone(zone);
  }

  @Override
  public Instant instant() {
    return delegate.instant();
  }

  public MutableClock asMutableClock() {
    return delegate;
  }
}
