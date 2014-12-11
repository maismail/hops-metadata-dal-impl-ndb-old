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
import com.mysql.clusterj.Query;
import com.mysql.clusterj.Results;
import se.sics.hop.exception.StorageException;

import java.util.List;
import java.util.Map;

public class HopsQuery<E> {
  private final Query<E> query;

  public HopsQuery(Query<E> query) {
    this.query = query;
  }

  public void setParameter(String s, Object o) throws StorageException {
    try {
      query.setParameter(s, o);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public List<E> getResultList() throws StorageException {
    try {
      return query.getResultList();
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public int deletePersistentAll() throws StorageException {
    try {
      return query.deletePersistentAll();
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public Results<E> execute(Object o) throws StorageException {
    try {
      return query.execute(o);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public Results<E> execute(Object... objects) throws StorageException {
    try {
      return query.execute(objects);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public Results<E> execute(Map<String, ?> map) throws StorageException {
    try {
      return query.execute(map);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public Map<String, Object> explain() throws StorageException {
    try {
      return query.explain();
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public void setLimits(long l, long l1) throws StorageException {
    try {
      query.setLimits(l, l1);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public void setOrdering(Query.Ordering ordering, String... strings)
      throws StorageException {
    try {
      query.setOrdering(ordering, strings);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }
}
