/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.avro;

import java.util.List;
import org.apache.avro.Schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Thrown when the expected contents of a union cannot be resolved. */
public class UnresolvedUnionException extends AvroRuntimeException {
  private static final Logger LOG = LoggerFactory.getLogger(UnresolvedUnionException.class);

  private Object unresolvedDatum;
  private Schema unionSchema;

  public UnresolvedUnionException(Schema unionSchema, Object unresolvedDatum) {
    super("Not in union: "+ unionSchema.getFullName() + " => "+unresolvedDatum);
    LOG.error("DatumType: " + unresolvedDatum.getClass().getName() + " and UnionSchemaTypes are: " +
      getUnionSchemaTypes(unionSchema));
    this.unionSchema = unionSchema;
    this.unresolvedDatum = unresolvedDatum;
  }

  private String getUnionSchemaTypes(Schema unionSchema) {
    StringBuilder builder = new StringBuilder("[");
    if(Type.UNION.equals(unionSchema.getType())) {
      List<Schema> types = unionSchema.getTypes();
      for(Schema type: types) {
        builder.append(type.getFullName()).append(", ");
      }
    }
    builder.append("]");
    return builder.toString();
  }

  public Object getUnresolvedDatum() {
    return unresolvedDatum;
  }

  public Schema getUnionSchema() {
    return unionSchema;
  }
}
