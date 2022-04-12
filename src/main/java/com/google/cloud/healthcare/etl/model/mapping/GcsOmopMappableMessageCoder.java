// Copyright 2020 Google LLC.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.google.cloud.healthcare.etl.model.mapping;

import org.apache.beam.sdk.coders.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/** Coder for {@link GcsOmopMappableMessage}. */
public class GcsOmopMappableMessageCoder extends CustomCoder<GcsOmopMappableMessage> {
  private static final NullableCoder<String> STRING_CODER =
      NullableCoder.of((Coder) StringUtf8Coder.of());

  public static GcsOmopMappableMessageCoder of() {
    return new GcsOmopMappableMessageCoder();
  }

  public void encode(GcsOmopMappableMessage value, OutputStream outStream)
      throws CoderException, IOException {
    STRING_CODER.encode(value.getData(), outStream);
  }

  public GcsOmopMappableMessage decode(InputStream inStream)
      throws CoderException, IOException {
    String data = STRING_CODER.decode(inStream);
    return new GcsOmopMappableMessage(data);
  }
}
