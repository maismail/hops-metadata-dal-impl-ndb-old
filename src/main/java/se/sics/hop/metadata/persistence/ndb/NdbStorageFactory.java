package se.sics.hop.metadata.persistence.ndb;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import se.sics.hop.metadata.persistence.DALStorageFactory;
import se.sics.hop.metadata.persistence.StorageConnector;
import se.sics.hop.metadata.persistence.dal.BlockTokenKeyDataAccess;
import se.sics.hop.metadata.persistence.dal.CorruptReplicaDataAccess;
import se.sics.hop.metadata.persistence.dal.EntityDataAccess;
import se.sics.hop.metadata.persistence.dal.ExcessReplicaDataAccess;
import se.sics.hop.metadata.persistence.dal.InvalidateBlockDataAccess;
import se.sics.hop.metadata.persistence.dal.LeaderDataAccess;
import se.sics.hop.metadata.persistence.dal.LeaseDataAccess;
import se.sics.hop.metadata.persistence.dal.LeasePathDataAccess;
import se.sics.hop.metadata.persistence.dal.ReplicaDataAccess;
import se.sics.hop.metadata.persistence.dal.StorageInfoDataAccess;
import se.sics.hop.metadata.persistence.dal.UnderReplicatedBlockDataAccess;
import se.sics.hop.metadata.persistence.dal.VariableDataAccess;
import se.sics.hop.metadata.persistence.ndb.dalimpl.BlockTokenKeyClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.CorruptReplicaClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.ExcessReplicaClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.InvalidatedBlockClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.LeaderClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.LeaseClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.LeasePathClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.ReplicaClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.StorageInfoClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.UnderReplicatedBlockClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.VariableClusterj;
import se.sics.hop.metadata.persistence.ndb.mysqlserver.MysqlServerConnector;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class NdbStorageFactory implements DALStorageFactory {

  private Map<Class, EntityDataAccess> dataAccessMap = new HashMap<Class, EntityDataAccess>();

  public void setConfiguration(Properties conf) {
    ClusterjConnector.getInstance().setConfiguration(conf);
    MysqlServerConnector.getInstance().setConfiguration(conf);
    initDataAccessMap();
  }

  private void initDataAccessMap() {
    dataAccessMap.put(LeaseDataAccess.class, new LeaseClusterj());
    dataAccessMap.put(LeasePathDataAccess.class, new LeasePathClusterj());
    dataAccessMap.put(LeaderDataAccess.class, new LeaderClusterj());
    dataAccessMap.put(ReplicaDataAccess.class, new ReplicaClusterj());
    dataAccessMap.put(CorruptReplicaDataAccess.class, new CorruptReplicaClusterj());
    dataAccessMap.put(ExcessReplicaDataAccess.class, new ExcessReplicaClusterj());
    dataAccessMap.put(InvalidateBlockDataAccess.class, new InvalidatedBlockClusterj());
    dataAccessMap.put(UnderReplicatedBlockDataAccess.class, new UnderReplicatedBlockClusterj());
    dataAccessMap.put(VariableDataAccess.class, new VariableClusterj());
    dataAccessMap.put(BlockTokenKeyDataAccess.class, new BlockTokenKeyClusterj());
    dataAccessMap.put(StorageInfoDataAccess.class, new StorageInfoClusterj());
  }

  public StorageConnector getConnector() {
    return ClusterjConnector.getInstance();
  }

  public EntityDataAccess getDataAccess(Class type) {
    return dataAccessMap.get(type);
  }
}
