package se.sics.hop.metadata.ndb;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import se.sics.hop.DALStorageFactory;
import se.sics.hop.StorageConnector;
import se.sics.hop.metadata.dal.BlockInfoDataAccess;
import se.sics.hop.metadata.dal.CorruptReplicaDataAccess;
import se.sics.hop.metadata.dal.EntityDataAccess;
import se.sics.hop.metadata.dal.ExcessReplicaDataAccess;
import se.sics.hop.metadata.dal.INodeAttributesDataAccess;
import se.sics.hop.metadata.dal.INodeDataAccess;
import se.sics.hop.metadata.dal.InvalidateBlockDataAccess;
import se.sics.hop.metadata.dal.LeaderDataAccess;
import se.sics.hop.metadata.dal.LeaseDataAccess;
import se.sics.hop.metadata.dal.LeasePathDataAccess;
import se.sics.hop.metadata.dal.PendingBlockDataAccess;
import se.sics.hop.metadata.dal.ReplicaDataAccess;
import se.sics.hop.metadata.dal.ReplicaUnderConstructionDataAccess;
import se.sics.hop.metadata.dal.UnderReplicatedBlockDataAccess;
import se.sics.hop.metadata.dal.VariableDataAccess;
import se.sics.hop.exception.StorageInitializtionException;
import se.sics.hop.metadata.ndb.dalimpl.BlockInfoClusterj;
import se.sics.hop.metadata.ndb.dalimpl.CorruptReplicaClusterj;
import se.sics.hop.metadata.ndb.dalimpl.ExcessReplicaClusterj;
import se.sics.hop.metadata.ndb.dalimpl.INodeAttributesClusterj;
import se.sics.hop.metadata.ndb.dalimpl.INodeClusterj;
import se.sics.hop.metadata.ndb.dalimpl.InvalidatedBlockClusterj;
import se.sics.hop.metadata.ndb.dalimpl.LeaderClusterj;
import se.sics.hop.metadata.ndb.dalimpl.LeaseClusterj;
import se.sics.hop.metadata.ndb.dalimpl.LeasePathClusterj;
import se.sics.hop.metadata.ndb.dalimpl.PendingBlockClusterj;
import se.sics.hop.metadata.ndb.dalimpl.ReplicaClusterj;
import se.sics.hop.metadata.ndb.dalimpl.ReplicaUnderConstructionClusterj;
import se.sics.hop.metadata.ndb.dalimpl.UnderReplicatedBlockClusterj;
import se.sics.hop.metadata.ndb.dalimpl.VariableClusterj;
import se.sics.hop.metadata.ndb.mysqlserver.MysqlServerConnector;

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
