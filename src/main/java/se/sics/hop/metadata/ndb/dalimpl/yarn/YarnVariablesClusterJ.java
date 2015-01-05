package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.YarnVariables;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;
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
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public YarnVariables findById(int id) throws StorageException {

        HopsSession session = connector.obtainSession();
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
        HopsSession session = connector.obtainSession();
        try {
            if (removed != null) {
                List<YarnVariablesDTO> toRemove = new ArrayList<YarnVariablesDTO>();
                for (YarnVariables entry : removed) {
                    toRemove.add(session.newInstance(YarnVariablesDTO.class, entry.getId()));
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                List<YarnVariablesDTO> toModify = new ArrayList<YarnVariablesDTO>();
                for (YarnVariables entry : modified) {
                    toModify.add(createPersistable(entry, session));
                }
                session.savePersistentAll(toModify);
            }
        } catch (Exception e) {
            throw new StorageException("Error while modifying invokerequests, error:" + e.getMessage());
        }
    }

    private YarnVariablesDTO createPersistable(YarnVariables yarnVariables, HopsSession session) throws StorageException {
        YarnVariablesDTO yarnDTO = session.newInstance(YarnVariablesDTO.class);
        yarnDTO.setid(yarnVariables.getId());
        yarnDTO.setvalue(yarnVariables.getValue());
        return yarnDTO;
    }
}
