package se.sics.hop.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.dal.StorageIdMapDataAccess;
import se.sics.hop.metadata.hdfs.entity.hop.HopStorageId;
import se.sics.hop.metadata.hdfs.tabledef.StorageIdMapTableDef;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.DBSession;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class StorageIdMapClusterj implements StorageIdMapTableDef, StorageIdMapDataAccess<HopStorageId> {

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
    try {
      DBSession dbSession = connector.obtainSession();
      StorageIdDTO sdto = dbSession.getSession().newInstance(StorageIdDTO.class);
      sdto.setSId(s.getsId());
      sdto.setStorageId(s.getStorageId());
      dbSession.getSession().savePersistent(sdto);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public HopStorageId findByPk(String storageId) throws StorageException {
    try {
      DBSession dbSession = connector.obtainSession();
      StorageIdDTO sdto = dbSession.getSession().find(StorageIdDTO.class, storageId);
      if(sdto == null)
        return null;
      return convert(sdto);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public Collection<HopStorageId> findAll() throws StorageException {
    try {
      DBSession dbSession = connector.obtainSession();
      QueryBuilder qb = dbSession.getSession().getQueryBuilder();
      QueryDomainType<StorageIdDTO> qdt = qb.createQueryDefinition(StorageIdDTO.class);
      Query<StorageIdDTO> q = dbSession.getSession().createQuery(qdt);
      return convert(q.getResultList());
    } catch (Exception e) {
      throw new StorageException(e);
    }
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
