package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.YarnVariables;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.YarnVariablesDataAccess;
import se.sics.hop.metadata.yarn.tabledef.YarnVariablesTableDef;

/**
 *
 * @author teo
 */
public class YarnVariablesClusterJ implements YarnVariablesTableDef, YarnVariablesDataAccess<YarnVariables> {

    @PersistenceCapable(table = "hayarn_variables")
    public interface YarnVariablesDTO {

        @PrimaryKey
        @Column(name = ID)
        int getId();

        void setId(int id);

        @Column(name = LAST_GIVEN_ID)
        int getLastGivenId();

        void setLastGivenId(int lastgivenid);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public int getNodeId() throws StorageException {
        int id;
        Session session = null;

        try {
            session = connector.obtainSession();
            System.out.println("VariablesClusterJ :: getNodeId() - session=" + session.toString());
            QueryBuilder builder = session.getQueryBuilder();
            QueryDomainType<YarnVariablesDTO> domain = builder.createQueryDefinition(YarnVariablesDTO.class);
            Query<YarnVariablesDTO> query = session.createQuery(domain);
            YarnVariablesDTO result = query.getResultList().get(0);
            id = result.getLastGivenId();
            result.setLastGivenId(id++);
            session.updatePersistent(result);

        } finally {
            if (session != null) {
                session.flush();
                session.close();
            }
        }
        return 0;

    }
}
