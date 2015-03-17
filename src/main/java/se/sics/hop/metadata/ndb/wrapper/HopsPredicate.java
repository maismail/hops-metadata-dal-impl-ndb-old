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
