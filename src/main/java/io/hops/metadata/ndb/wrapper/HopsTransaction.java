package io.hops.metadata.ndb.wrapper;

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.Transaction;
import io.hops.exception.StorageException;

public class HopsTransaction {
  private final Transaction transaction;

  public HopsTransaction(Transaction transaction) {
    this.transaction = transaction;
  }

  public void begin() throws StorageException {
    try {
      transaction.begin();
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public void commit() throws StorageException {
    try {
      transaction.commit();
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public void rollback() throws StorageException {
    try {
      transaction.rollback();
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public boolean isActive() throws StorageException {
    try {
      return transaction.isActive();
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public void setRollbackOnly() throws StorageException {
    try {
      transaction.setRollbackOnly();
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public boolean getRollbackOnly() throws StorageException {
    try {
      return transaction.getRollbackOnly();
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }
}
