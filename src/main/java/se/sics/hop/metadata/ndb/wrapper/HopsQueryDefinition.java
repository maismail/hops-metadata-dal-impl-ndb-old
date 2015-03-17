package se.sics.hop.metadata.ndb.wrapper;

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.PredicateOperand;
import com.mysql.clusterj.query.QueryDefinition;
import se.sics.hop.exception.StorageException;

public class HopsQueryDefinition<E> {
  private final QueryDefinition<E> queryDefinition;

  public HopsQueryDefinition(QueryDefinition<E> queryDefinition) {
    this.queryDefinition = queryDefinition;
  }

  public HopsQueryDefinition<E> where(HopsPredicate predicate)
      throws StorageException {
    try {
      return new HopsQueryDefinition<E>(queryDefinition.where(predicate
          .getPredicate()));
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public HopsPredicateOperand param(String s) throws StorageException {
    try {
      return new HopsPredicateOperand(queryDefinition.param(s));
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public HopsPredicate not(HopsPredicate predicate) throws StorageException {
    try {
      return new HopsPredicate(queryDefinition.not(predicate.getPredicate()));
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }
}
