package se.sics.hop.metadata.ndb;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import se.sics.hop.DALStorageFactory;
import se.sics.hop.StorageConnector;
import se.sics.hop.metadata.hdfs.dal.BlockInfoDataAccess;
import se.sics.hop.metadata.hdfs.dal.CorruptReplicaDataAccess;
import se.sics.hop.metadata.hdfs.dal.EntityDataAccess;
import se.sics.hop.metadata.hdfs.dal.ExcessReplicaDataAccess;
import se.sics.hop.metadata.hdfs.dal.INodeAttributesDataAccess;
import se.sics.hop.metadata.hdfs.dal.INodeDataAccess;
import se.sics.hop.metadata.hdfs.dal.InvalidateBlockDataAccess;
import se.sics.hop.metadata.hdfs.dal.LeaderDataAccess;
import se.sics.hop.metadata.hdfs.dal.LeaseDataAccess;
import se.sics.hop.metadata.hdfs.dal.LeasePathDataAccess;
import se.sics.hop.metadata.hdfs.dal.PendingBlockDataAccess;
import se.sics.hop.metadata.hdfs.dal.ReplicaDataAccess;
import se.sics.hop.metadata.hdfs.dal.ReplicaUnderConstructionDataAccess;
import se.sics.hop.metadata.hdfs.dal.UnderReplicatedBlockDataAccess;
import se.sics.hop.metadata.hdfs.dal.VariableDataAccess;
import se.sics.hop.exception.StorageInitializtionException;
import se.sics.hop.metadata.hdfs.dal.StorageIdMapDataAccess;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.BlockInfoClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.CorruptReplicaClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.ExcessReplicaClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.INodeAttributesClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.INodeClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.InvalidatedBlockClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.LeaderClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.LeaseClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.LeasePathClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.PendingBlockClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.ReplicaClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.ReplicaUnderConstructionClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.StorageIdMapClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.UnderReplicatedBlockClusterj;
import se.sics.hop.metadata.ndb.dalimpl.hdfs.VariableClusterj;
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
