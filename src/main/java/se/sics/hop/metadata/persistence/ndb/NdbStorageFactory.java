package se.sics.hop.metadata.persistence.ndb;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import se.sics.hop.metadata.persistence.DALStorageFactory;
import se.sics.hop.metadata.persistence.StorageConnector;
import se.sics.hop.metadata.persistence.dal.BlockInfoDataAccess;
import se.sics.hop.metadata.persistence.dal.BlockTokenKeyDataAccess;
import se.sics.hop.metadata.persistence.dal.CorruptReplicaDataAccess;
import se.sics.hop.metadata.persistence.dal.EntityDataAccess;
import se.sics.hop.metadata.persistence.dal.ExcessReplicaDataAccess;
import se.sics.hop.metadata.persistence.dal.INodeAttributesDataAccess;
import se.sics.hop.metadata.persistence.dal.INodeDataAccess;
import se.sics.hop.metadata.persistence.dal.InvalidateBlockDataAccess;
import se.sics.hop.metadata.persistence.dal.LeaderDataAccess;
import se.sics.hop.metadata.persistence.dal.LeaseDataAccess;
import se.sics.hop.metadata.persistence.dal.LeasePathDataAccess;
import se.sics.hop.metadata.persistence.dal.PendingBlockDataAccess;
import se.sics.hop.metadata.persistence.dal.ReplicaDataAccess;
import se.sics.hop.metadata.persistence.dal.ReplicaUnderConstructionDataAccess;
import se.sics.hop.metadata.persistence.dal.UnderReplicatedBlockDataAccess;
import se.sics.hop.metadata.persistence.dal.VariableDataAccess;
import se.sics.hop.metadata.persistence.exceptions.StorageInitializtionException;
import se.sics.hop.metadata.persistence.ndb.dalimpl.BlockInfoClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.BlockTokenKeyClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.CorruptReplicaClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.ExcessReplicaClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.INodeAttributesClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.INodeClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.InvalidatedBlockClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.LeaderClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.LeaseClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.LeasePathClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.PendingBlockClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.ReplicaClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.ReplicaUnderConstructionClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.UnderReplicatedBlockClusterj;
import se.sics.hop.metadata.persistence.ndb.dalimpl.VariableClusterj;
import se.sics.hop.metadata.persistence.ndb.mysqlserver.MysqlServerConnector;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class NdbStorageFactory implements DALStorageFactory {

  private Map<Class, EntityDataAccess> dataAccessMap = new HashMap<Class, EntityDataAccess>();

  @Override
  public void setConfiguration(String configFile) throws StorageInitializtionException{
    try {
      Properties conf = new Properties();
      InputStream inStream = this.getClass().getClassLoader().getResourceAsStream(configFile);
      conf.load(inStream);
      ClusterjConnector.getInstance().setConfiguration(conf);
      MysqlServerConnector.getInstance().setConfiguration(conf);
      initDataAccessMap();
    } catch (IOException ex) {
     throw new StorageInitializtionException(ex);
    }
  }

  private void initDataAccessMap() {
    dataAccessMap.put(BlockInfoDataAccess.class, new BlockInfoClusterj());
    dataAccessMap.put(PendingBlockDataAccess.class, new PendingBlockClusterj());
    dataAccessMap.put(ReplicaUnderConstructionDataAccess.class, new ReplicaUnderConstructionClusterj());
    dataAccessMap.put(INodeDataAccess.class, new INodeClusterj());
    dataAccessMap.put(INodeAttributesDataAccess.class, new INodeAttributesClusterj());
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
  }

  @Override
  public StorageConnector getConnector() {
    return ClusterjConnector.getInstance();
  }

  @Override
  public EntityDataAccess getDataAccess(Class type) {
    return dataAccessMap.get(type);
  }
}
