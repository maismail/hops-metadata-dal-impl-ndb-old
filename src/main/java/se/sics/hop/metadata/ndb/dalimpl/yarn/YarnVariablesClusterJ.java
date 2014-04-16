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
        int getid();

        void setid(int id);

        @Column(name = VALUE)
        int getvalue();

        void setvalue(int value);
        /*@Column(name = LAST_UPDATEDCONTAINERINFO_ID)
        int getlastupdatedcontainerinfoid();

        void setlastupdatedcontainerinfoid(int lastupdatedcontainerinfoid);

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

        @Column(name = LAST_NODEHBRESPONSE_ID)
        int getlastnodehbresponseid();

        void setlastnodehbresponseid(int lastnodehbresponseid);

        @Column(name = LAST_RMCONTEXT_ID)
        int getlastrmcontextid();

        void setlastrmcontextid(int lastrmcontextid);

        @Column(name = LAST_CONTAINERSTATUS_ID)
        int getlastcontainerstatusid();

        void setlastcontainerstatusid(int lastrmcontainerstatusid);

        @Column(name = LAST_CONTAINERID_ID)
        int getlastcontaineridid();

        void setlastcontaineridid(int lastcontaineridid);

        @Column(name = LAST_APPATTEMPTID_ID)
        int getlastappattemptidid();

        void setlastappattemptidid(int lastappattemptidid);

        @Column(name = LAST_APPLICATIONID_ID)
        int getlastapplicationidid();

        void setlastapplicationidid(int lastapplicationidid);*/
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public YarnVariables findById(int id) throws StorageException {

        Session session = connector.obtainSession();
        YarnVariablesDTO yarnDTO = null;
        if (session != null) {
            if(id == Integer.MIN_VALUE){
                id = idVal;
            }
            yarnDTO = session.find(YarnVariablesDTO.class, id);
        }
        if (yarnDTO == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }
        YarnVariables objFound = new YarnVariables(yarnDTO.getid(), yarnDTO.getvalue());
        //YarnVariables objFound = new YarnVariables(yarnDTO.getid(), yarnDTO.getlastupdatedcontainerinfoid(), yarnDTO.getlastnodeidid(), yarnDTO.getlastnodeid(), yarnDTO.getlastresourceid(), yarnDTO.getlastlistid(), yarnDTO.getlastnodehbresponseid(), yarnDTO.getlastrmcontextid(), yarnDTO.getlastcontainerstatusid(), yarnDTO.getlastcontaineridid(), yarnDTO.getlastappattemptidid(), yarnDTO.getlastapplicationidid());
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
        yarnDTO.setid(yarnVariables.getId());
        yarnDTO.setvalue(yarnVariables.getValue());
        /*yarnDTO.setlastupdatedcontainerinfoid(yarnVariables.getLastupdatedcontainerinfoid());
        yarnDTO.setlastnodeidid(yarnVariables.getLastnodeidId());
        yarnDTO.setlastnodeid(yarnVariables.getLastnodeId());
        yarnDTO.setlastresourceid(yarnVariables.getLastresourceId());
        yarnDTO.setlastlistid(yarnVariables.getLastlistid());
        yarnDTO.setlastnodehbresponseid(yarnVariables.getLastnodehbresponseid());
        yarnDTO.setlastrmcontextid(yarnVariables.getLastrmcontextid());
        yarnDTO.setlastcontainerstatusid(yarnVariables.getLastcontainerstatusid());
        yarnDTO.setlastcontaineridid(yarnVariables.getLastcontaineridId());
        yarnDTO.setlastappattemptidid(yarnVariables.getLastappattemptidId());
        yarnDTO.setlastapplicationidid(yarnVariables.getLastapplicationidId());*/
        session.savePersistent(yarnDTO);
        return yarnDTO;
    }
}
