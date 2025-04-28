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
package org.apache.polaris.service.auth;

import com.google.common.base.Preconditions;
import java.util.Optional;

public record TokenDecodeResult(Status status, Optional<DecodedToken> token) {

  public TokenDecodeResult {
    Preconditions.checkState(!(status == Status.SUCCESS && token.isEmpty()));
    Preconditions.checkState(!(status != Status.SUCCESS && token.isPresent()));
  }

  public enum Status {
    SUCCESS(""),
    MALFORMED_TOKEN("Malformed token"),
    INVALID_ISSUER("Invalid issuer"),
    INVALID_REALM("Invalid realm"),
    ;
    private final String message;

    Status(String message) {
      this.message = message;
    }

    public String message() {
      return message;
    }
  }
}
