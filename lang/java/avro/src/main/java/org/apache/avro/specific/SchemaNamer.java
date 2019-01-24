/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.avro.specific;

public class SchemaNamer {

  private final String prefixToAttach;
  private final String prefixToMatch;
  private final String prefixToIgnore;
  public static final SchemaNamer DEFAULT_SCHEMA_NAMER = new SchemaNamer("", "", "");

  public SchemaNamer(String prefixToAttach, String prefixToMatch, String prefixToIgnore) {
    this.prefixToAttach = prefixToAttach;
    this.prefixToMatch = prefixToMatch;
    this.prefixToIgnore = prefixToIgnore;
  }

  public String getName(Class<?> clazz) {
    return clazz.getSimpleName();
  }

  public String getFullName(Class<?> clazz) {
    Package pkg = clazz.getPackage();
    return prefixName(pkg, clazz.getName());
  }

  private String prefixName(Package pkg, String currName) {
    if (pkg == null) {
      return currName;
    }
    String pkgName = pkg.getName();
    if (prefixToAttach != null && !"".equals(prefixToAttach) &&
      pkgName.startsWith(prefixToMatch) && !pkgName.startsWith(prefixToIgnore)) {
      return prefixToAttach + "." + currName;
    } else {
      return currName;
    }
  }

  public String getNamespace(Class<?> clazz) {
    return getNamespace(clazz.getPackage());
  }

  public String getNamespace(Package pkg) {
    return prefixName(pkg, pkg.getName());
  }
}
