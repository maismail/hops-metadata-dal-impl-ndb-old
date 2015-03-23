package io.hops.metadata.ndb.wrapper;

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.SessionFactory;
import io.hops.exception.StorageException;

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
}
