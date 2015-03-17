package se.sics.hop.metadata.ndb.wrapper;

import com.mysql.clusterj.ClusterJDatastoreException;
import com.mysql.clusterj.ClusterJException;
import se.sics.hop.exception.StorageException;
import se.sics.hop.exception.TransientStorageException;

public class HopsExceptionHelper {
  public static StorageException wrap(ClusterJException e) {
    if (isTransient(e)) {
      return new TransientStorageException(e);
    } else {
      return new StorageException(e);
    }
  }

  private static boolean isTransient(ClusterJException e) {
    if (e instanceof ClusterJDatastoreException) {
      // TODO identify transient exceptions
      // http://dev.mysql.com/doc/ndbapi/en/ndb-error-codes.html
      if (e.getMessage().contains("code 266")) { // dead lock
        return true;
      } else if (e.getMessage().contains("code 274")) { // dead lock.Transaction had timed out when trying to commit it
        return true;
      } else if (e.getMessage().contains("code 245")) { // too many active scans errors
        return true;
      } else if (e.getMessage().contains("code 146")) { //  message Time-out in NDB, probably caused by deadlock .
        return true;
      }
    }
    return false;
  }
}
