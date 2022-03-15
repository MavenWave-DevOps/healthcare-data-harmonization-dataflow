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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents OMOP metadata from the HCLS API for mapping. This class is meant to wrap the original
 * response from the API to be consumed by the mapping library. The ID is unused in this class.
 */
public class HclsApiOmopMappableMessage implements Mappable {
  private static final Logger LOG = LoggerFactory.getLogger(HclsApiOmopMappableMessage.class);
  private final String schematizedData;

  public HclsApiOmopMappableMessage(String schematizedData) {
    this.schematizedData = schematizedData;
    LOG.info("schematizedData: "+schematizedData);
  }

  public String getId() {
    return null;
  }

  public String getData() {
    LOG.info("getData: "+this.schematizedData);
    return this.schematizedData;
  }

  public static HclsApiOmopMappableMessage from(String data) {
    LOG.info("from method: "+data);
    return new HclsApiOmopMappableMessage(data);
  }
}
