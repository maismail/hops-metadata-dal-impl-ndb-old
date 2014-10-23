package se.sics.hop.metadata.ndb;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import se.sics.hop.DALStorageFactory;
import se.sics.hop.StorageConnector;
import se.sics.hop.metadata.hdfs.dal.*;
import se.sics.hop.exception.StorageInitializtionException;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.*;
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
    dataAccessMap.put(StorageIdMapDataAccess.class, new StorageIdMapClusterj());
    dataAccessMap.put(BlockLookUpDataAccess.class, new BlockLookUpClusterj());
    dataAccessMap.put(QuotaUpdateDataAccess.class, new QuotaUpdateClusterj());
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
