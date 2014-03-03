package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
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
        YarnVariables objFound = new YarnVariables(yarnDTO.getId(), yarnDTO.getLastupdatedcontainerinfoid());

        return objFound;
        /*QueryBuilder builder = session.getQueryBuilder();
         QueryDomainType<YarnVariablesDTO> domain = builder.createQueryDefinition(YarnVariablesDTO.class);
         Query<YarnVariablesDTO> query = session.createQuery(domain);
         YarnVariablesDTO result = query.getResultList().get(0);
         lastupdatedcontainerinfoid = result.getLastupdatedcontainerinfoid();
         result.setLastupdatedcontainerinfoid(lastupdatedcontainerinfoid++);
         session.updatePersistent(result);*/
        //return lastupdatedcontainerinfoid;

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
        YarnVariables objFound = new YarnVariables(yarnDTO.getId(), yarnDTO.getLastupdatedcontainerinfoid());
        YarnVariablesDTO newDTO = session.newInstance(YarnVariablesDTO.class);
        newDTO.setId(idVal);
        int newid = objFound.getLastupdatedcontainerinfoid() + 1;
        newDTO.setLastupdatedcontainerinfoid(newid);
        session.savePersistent(newDTO);
        return objFound;
    }
}
