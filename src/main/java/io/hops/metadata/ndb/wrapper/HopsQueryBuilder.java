package io.hops.metadata.ndb.wrapper;

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import io.hops.exception.StorageException;

public class HopsQueryBuilder {
  private final QueryBuilder queryBuilder;

  public HopsQueryBuilder(QueryBuilder queryBuilder) {
    this.queryBuilder = queryBuilder;
  }

  public <T> HopsQueryDomainType<T> createQueryDefinition(Class<T> aClass)
      throws StorageException {
    try {
      QueryDomainType<T> queryDomainType =
          queryBuilder.createQueryDefinition(aClass);
      return new HopsQueryDomainType<T>(queryDomainType);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }
}
