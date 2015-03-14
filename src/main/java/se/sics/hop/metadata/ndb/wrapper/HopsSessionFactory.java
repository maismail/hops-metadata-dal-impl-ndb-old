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
import com.mysql.clusterj.SessionFactory;
import se.sics.hop.exception.StorageException;

import java.util.Map;

public class HopsSessionFactory {
  private final SessionFactory factory;

  public HopsSessionFactory(SessionFactory factory) {
    this.factory = factory;
  }

  public HopsSession getSession() throws StorageException {
    try {
      return new HopsSession(factory.getSession());
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public HopsSession getSession(Map map) throws StorageException {
    try {
      return new HopsSession(factory.getSession(map));
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public void close() throws StorageException {
    try {
      factory.close();
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }
  
  public void releaseResourcesForThisThread(){
      factory.releaseResourcesForThisThread();
  }
  
}
