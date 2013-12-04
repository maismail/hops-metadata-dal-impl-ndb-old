package se.sics.hop.metadata.persistence.ndb;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import se.sics.hop.metadata.persistence.DALStorageFactory;
import se.sics.hop.metadata.persistence.StorageConnector;
import se.sics.hop.metadata.persistence.dal.EntityDataAccess;
import se.sics.hop.metadata.persistence.dal.LeaseDataAccess;
import se.sics.hop.metadata.persistence.ndb.dalimpl.LeaseClusterj;
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
  }

  public StorageConnector getConnector() {
    return ClusterjConnector.getInstance();
  }

  public EntityDataAccess getDataAccess(Class type) {
    return dataAccessMap.get(type);
  }
}
