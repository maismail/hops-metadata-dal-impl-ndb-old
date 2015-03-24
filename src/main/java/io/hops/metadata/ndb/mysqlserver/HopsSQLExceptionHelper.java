package io.hops.metadata.ndb.mysqlserver;

import io.hops.exception.StorageException;
import io.hops.exception.TransientStorageException;

import java.sql.SQLException;

public class HopsSQLExceptionHelper {
  public static StorageException wrap(SQLException e) {
    if (isTransient(e)) {
      return new TransientStorageException(e);
    } else {
      return new StorageException(e);
    }
  }

  private static boolean isTransient(SQLException e) {
    // TODO identify transient exceptions
    return false;
  }
}
