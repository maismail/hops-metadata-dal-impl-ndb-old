package io.hops.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.dal.StorageIdMapDataAccess;
import io.hops.metadata.hdfs.entity.hop.HopStorageId;
import io.hops.metadata.hdfs.tabledef.StorageIdMapTableDef;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;

public class StorageIdMapClusterj implements StorageIdMapTableDef,
    StorageIdMapDataAccess<HopStorageId> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface StorageIdDTO {
    @PrimaryKey
    @Column(name = STORAGE_ID)
    String getStorageId();

    void setStorageId(String storageId);

    @Column(name = SID)
    int getSId();

    void setSId(int sId);
  }
  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public void add(HopStorageId s) throws StorageException {
    HopsSession session = connector.obtainSession();
    StorageIdDTO sdto = session.newInstance(StorageIdDTO.class);
    sdto.setSId(s.getsId());
    sdto.setStorageId(s.getStorageId());
    session.savePersistent(sdto);
  }

  @Override
  public HopStorageId findByPk(String storageId) throws StorageException {
    HopsSession session = connector.obtainSession();
    StorageIdDTO sdto = session.find(StorageIdDTO.class, storageId);
    if(sdto == null)
      return null;
    return convert(sdto);
  }

  @Override
  public Collection<HopStorageId> findAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<StorageIdDTO> qdt = qb.createQueryDefinition(StorageIdDTO.class);
    HopsQuery<StorageIdDTO> q = session.createQuery(qdt);
    return convert(q.getResultList());
  }

  private Collection<HopStorageId> convert(List<StorageIdDTO> dtos) {
    List<HopStorageId> hopstorageId = new ArrayList<HopStorageId>();
    for (StorageIdDTO sdto : dtos) {
      hopstorageId.add(convert(sdto));
    }
    return hopstorageId;
  }

  private HopStorageId convert(StorageIdDTO sdto) {
    return new HopStorageId(sdto.getStorageId(), sdto.getSId());
  }
}
