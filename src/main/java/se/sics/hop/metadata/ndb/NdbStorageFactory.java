package se.sics.hop.metadata.ndb;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import se.sics.hop.DALStorageFactory;
import se.sics.hop.StorageConnector;
import se.sics.hop.exception.StorageInitializtionException;

import se.sics.hop.metadata.hdfs.dal.EntityDataAccess;
import se.sics.hop.metadata.ndb.dalimpl.yarn.ApplicationAttemptStateClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.ApplicationStateClusterJ;
import se.sics.hop.metadata.ndb.dalimpl.yarn.RMStateVersionClusterJ;
import se.sics.hop.metadata.yarn.dal.RMStateVersionDataAccess;
import se.sics.hop.metadata.ndb.mysqlserver.MysqlServerConnector;
import se.sics.hop.metadata.yarn.dal.ApplicationAttemptStateDataAccess;
import se.sics.hop.metadata.yarn.dal.ApplicationStateDataAccess;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class NdbStorageFactory implements DALStorageFactory {

    private Map<Class, EntityDataAccess> dataAccessMap = new HashMap<Class, EntityDataAccess>();

    @Override
    public void setConfiguration(String configFile) throws StorageInitializtionException {
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
        //RM STATE STORE
        dataAccessMap.put(RMStateVersionDataAccess.class, new RMStateVersionClusterJ());
        dataAccessMap.put(ApplicationStateDataAccess.class, new ApplicationStateClusterJ());
        dataAccessMap.put(ApplicationAttemptStateDataAccess.class, new ApplicationAttemptStateClusterJ());

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
