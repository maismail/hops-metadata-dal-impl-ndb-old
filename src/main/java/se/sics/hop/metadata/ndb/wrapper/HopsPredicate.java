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
package se.sics.hop.metadata.ndb.wrapper;

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.query.Predicate;
import se.sics.hop.exception.StorageException;

public class HopsPredicate {
  private final Predicate predicate;

  public HopsPredicate(Predicate predicate) {
    this.predicate = predicate;
  }

  public HopsPredicate or(HopsPredicate predicate) throws StorageException {
    try {
      Predicate predicate1 = this.predicate.or(predicate.getPredicate());
      return new HopsPredicate(predicate1);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public HopsPredicate and(HopsPredicate predicate) throws StorageException {
    try {
      Predicate predicate1 = this.predicate.and(predicate.getPredicate());
      return new HopsPredicate(predicate1);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public HopsPredicate not() throws StorageException {
    try {
      Predicate predicate1 = this.predicate.not();
      return new HopsPredicate(predicate1);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  Predicate getPredicate() {
    return predicate;
  }
}
