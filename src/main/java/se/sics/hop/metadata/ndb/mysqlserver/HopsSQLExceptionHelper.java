package se.sics.hop.metadata.ndb.mysqlserver;

import com.mysql.clusterj.ClusterJDatastoreException;
import com.mysql.clusterj.ClusterJException;
import se.sics.hop.exception.StorageException;
import se.sics.hop.exception.TransientStorageException;

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
