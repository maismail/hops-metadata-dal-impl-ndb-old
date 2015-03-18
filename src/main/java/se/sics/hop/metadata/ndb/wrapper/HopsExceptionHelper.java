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
      // http://dev.mysql.com/doc/ndbapi/en/ndb-error-classifications.html
      // The classifications can be found in ndberror.h and ndberror.c in the ndb sources
      if (e.getMessage().contains("classification 7")) {
        // Temporary Resource error (TR)
        return true;
      } else if (e.getMessage().contains("classification 8")) {
        // Node Recovery error (NR)
        return true;
      } else if (e.getMessage().contains("classification 9")) {
        // Overload error (OL)
        return true;
      } else if (e.getMessage().contains("classification 10")) {
        // Timeout expired (TO)
        return true;
      } else if (e.getMessage().contains("classification 15")) {
        // Node shutdown (NS)
        return true;
      } else if (e.getMessage().contains("classification 18")) {
        // Internal temporary (IT)
        return true;
      }
    }
    return false;
  }
}
