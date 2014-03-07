package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.Collection;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.YarnVariables;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.YarnVariablesDataAccess;
import se.sics.hop.metadata.yarn.tabledef.YarnVariablesTableDef;

/**
 * Table with one row that is used to obtain unique ids for tables. This
 * solution can be dropped once ClusterJ implements auto-increment.
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class YarnVariablesClusterJ implements YarnVariablesTableDef, YarnVariablesDataAccess<YarnVariables> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface YarnVariablesDTO {

        @PrimaryKey
        @Column(name = ID)
        int getId();

        void setId(int id);

        @Column(name = LAST_UPDATEDCONTAINERINFO_ID)
        int getLastupdatedcontainerinfoid();

        void setLastupdatedcontainerinfoid(int lastupdatedcontainerinfoid);

        @Column(name = LAST_NODEID_ID)
        int getlastnodeidid();

        void setlastnodeidid(int lastnodeidid);

        @Column(name = LAST_NODE_ID)
        int getlastnodeid();

        void setlastnodeid(int lastnodeid);

        @Column(name = LAST_RESOURCE_ID)
        int getlastresourceid();

        void setlastresourceid(int lastresourceid);
        
        @Column(name = LAST_LIST_ID)
        int getlastlistid();

        void setlastlistid(int lastlistid);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public YarnVariables findById() throws StorageException {

        Session session = connector.obtainSession();
        YarnVariablesDTO yarnDTO = null;
        if (session != null) {
            yarnDTO = session.find(YarnVariablesDTO.class, idVal);
        }
        if (yarnDTO == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }
        YarnVariables objFound = new YarnVariables(yarnDTO.getId(), yarnDTO.getLastupdatedcontainerinfoid(), yarnDTO.getlastnodeidid(), yarnDTO.getlastnodeid(), yarnDTO.getlastresourceid(), yarnDTO.getlastlistid());
        return objFound;
    }

    @Override
    public YarnVariables findByIdIncrementUpdatedContainerInfo() throws StorageException {

        Session session = connector.obtainSession();
        YarnVariablesDTO yarnDTO = null;
        if (session != null) {
            yarnDTO = session.find(YarnVariablesDTO.class, idVal);
        }
        if (yarnDTO == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }
        YarnVariables objFound = new YarnVariables(yarnDTO.getId(), yarnDTO.getLastupdatedcontainerinfoid(), yarnDTO.getlastnodeidid(), yarnDTO.getlastnodeid(), yarnDTO.getlastresourceid(), yarnDTO.getlastlistid());
        YarnVariablesDTO newDTO = session.newInstance(YarnVariablesDTO.class);
        newDTO.setId(idVal);
        int newid = objFound.getLastupdatedcontainerinfoid() + 1;
        newDTO.setLastupdatedcontainerinfoid(newid);
        session.savePersistent(newDTO);
        return objFound;
    }

    @Override
    public void prepare(Collection<YarnVariables> modified, Collection<YarnVariables> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (YarnVariables hopApplicationId : removed) {
                    YarnVariablesDTO persistable = session.newInstance(YarnVariablesDTO.class, hopApplicationId.getId());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (YarnVariables hopAppAttemptId : modified) {
                    YarnVariablesDTO persistable = createPersistable(hopAppAttemptId, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    private YarnVariablesDTO createPersistable(YarnVariables yarnVariables, Session session) {
        YarnVariablesDTO yarnDTO = session.newInstance(YarnVariablesDTO.class);
        yarnDTO.setId(yarnVariables.getId());
        yarnDTO.setLastupdatedcontainerinfoid(yarnVariables.getLastupdatedcontainerinfoid());
        yarnDTO.setlastnodeidid(yarnVariables.getLastnodeidId());
        yarnDTO.setlastnodeid(yarnVariables.getLastnodeId());
        yarnDTO.setlastresourceid(yarnVariables.getLastresourceId());
        yarnDTO.setlastlistid(yarnVariables.getLastlistid());
        session.savePersistent(yarnDTO);
        return yarnDTO;
    }
}
