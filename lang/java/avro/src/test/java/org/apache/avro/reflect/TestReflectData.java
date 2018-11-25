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

package org.apache.avro.reflect;

import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SchemaNamer;
import org.junit.Test;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class TestReflectData {
  @Test
  @SuppressWarnings("unchecked")
  public void testWeakSchemaCaching() throws Exception {
    int numSchemas = 1000000;
    for (int i = 0; i < numSchemas; i++) {
      // Create schema
      Schema schema = Schema.createRecord("schema", null, null, false);
      schema.setFields(Collections.<Schema.Field>emptyList());

      ReflectData.get().getRecordState(new Object(), schema);
    }

    // Reflect the number of schemas currently in the cache
    ReflectData.ClassAccessorData classData = ReflectData.ACCESSOR_CACHE
        .get(Object.class);

    System.gc(); // Not guaranteed, but seems to be reliable enough

    assertThat("ReflectData cache should release references",
        classData.bySchema.size(), lessThan(numSchemas));
  }

  private class R1 {
    private int id;
    private List<String> names;

    public R1(int id, List<String> names) {
      this.id = id;
      this.names = names;
    }

    public int getId() {
      return id;
    }

    public List<String> getNames() {
      return names;
    }
  }

  @Test
  public void testSchemaNamer() {
    SchemaNamer schemaNamer = new SchemaNamer("prefix1", "org.apache");

    ReflectData reflectData = new ReflectData.AllowNull(schemaNamer);
    Schema schema = reflectData.getSchema(R1.class);
    String schemaJson = schema.toString();
    assertNotNull(schemaJson);

    Schema.Parser parser = new Parser();
    Schema parsedSchema = parser.parse(schemaJson);
    assertNotNull(parsedSchema);

    GenericData.Record record = new Record(schema);
    record.put("id", 123);

    Schema namesSchema = new Schema.Parser().parse("{\"name\":\"names\",\"type\":[\"null\",{\"type\":\"array\",\"items\":\"string\",\"java-class\":\"java.util.List\"}],\"default\":null}");

    ArrayList<String> names = new ArrayList<String>();
    names.add("foo");
    names.add("bar");

    GenericData.Array<String> namesArray = new GenericData.Array<String>(namesSchema, names);
    record.put("names", namesArray);

    boolean validate = reflectData.validate(schema, record);
    assertTrue(validate);
  }
}
