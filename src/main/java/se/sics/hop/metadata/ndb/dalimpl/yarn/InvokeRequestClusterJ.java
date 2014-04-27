package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.InvokeRequest;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.InvokeRequestDataAccess;
import se.sics.hop.metadata.yarn.tabledef.InvokeRequestTableDef;

/**
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class InvokeRequestClusterJ implements InvokeRequestTableDef, InvokeRequestDataAccess<InvokeRequest> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface InvokeRequestDTO {

        @PrimaryKey
        @Column(name = ID)
        int getid();

        void setid(int id);

        @Column(name = NODEID)
        int getnodeid();

        void setnodeid(int nodeid);

        @Column(name = TYPE)
        int gettype();

        void settype(int type);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public void createInvokeRequest(InvokeRequest invokeRequest) throws StorageException {
        Session session = connector.obtainSession();
        createPersistable(invokeRequest, session);
    }

    @Override
    public List<InvokeRequest> findAll() throws StorageException {
        Session session = connector.obtainSession();
        QueryBuilder qb = session.getQueryBuilder();
        QueryDomainType<InvokeRequestDTO> dobj = qb.createQueryDefinition(InvokeRequestDTO.class);
        Query<InvokeRequestDTO> query = session.createQuery(dobj);
        List<InvokeRequestDTO> results = query.getResultList();
        try {
            return createHopInvokeRequestsList(results);
        } catch (IOException ex) {
            Logger.getLogger(ApplicationIdClusterJ.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    @Override
    public void prepare(Collection<InvokeRequest> modified, Collection<InvokeRequest> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (InvokeRequest hopInvokeRequests : removed) {
                    InvokeRequestDTO persistable = session.newInstance(InvokeRequestDTO.class, hopInvokeRequests.getId());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (InvokeRequest hopAppAttemptId : modified) {
                    InvokeRequestDTO persistable = createPersistable(hopAppAttemptId, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    @Override
    public void deleteAll(int startId, int endId) throws StorageException {
    
    Session session = connector.obtainSession();
        //session.deletePersistentAll(NodeIdDTO.class);
        for (int i = startId; i < endId; i++) {
            QueryBuilder qb = session.getQueryBuilder();

            QueryDomainType<InvokeRequestDTO> dobj = qb.createQueryDefinition(InvokeRequestDTO.class);
            Predicate pred1 = dobj.get("id").equal(dobj.param("id"));
            dobj.where(pred1);
            Query<InvokeRequestDTO> query = session.createQuery(dobj);
            query.setParameter("id", i);

            List<InvokeRequestDTO> results = query.getResultList();
            try {
                InvokeRequestDTO del = session.newInstance(InvokeRequestDTO.class);
                del.setid(results.get(0).getid());
                del.setnodeid(results.get(0).getnodeid());
                del.settype(results.get(0).gettype());
                InvokeRequestDTO nodeidDTO = session.find(InvokeRequestDTO.class, del.getid());

                session.deletePersistent(nodeidDTO);
            } catch (Exception e) {
            }
        }
    }

    private InvokeRequestDTO createPersistable(InvokeRequest hopInvokeRequests, Session session) {
        InvokeRequestDTO invokerequestsDTO = session.newInstance(InvokeRequestDTO.class);
        invokerequestsDTO.setid(hopInvokeRequests.getId());
        invokerequestsDTO.setnodeid(hopInvokeRequests.getNodeid());
        invokerequestsDTO.settype(hopInvokeRequests.getType());
        session.savePersistent(invokerequestsDTO);
        return invokerequestsDTO;
    }

    private List<InvokeRequest> createHopInvokeRequestsList(List<InvokeRequestDTO> list) throws IOException {
        List<InvokeRequest> hopInvokeRequests = new ArrayList<InvokeRequest>();
        for (InvokeRequestDTO persistable : list) {
            hopInvokeRequests.add(createHopInvokeRequests(persistable));
        }
        return hopInvokeRequests;
    }

    private InvokeRequest createHopInvokeRequests(InvokeRequestDTO invokerequestsDTO) {
        return new InvokeRequest(invokerequestsDTO.getid(), invokerequestsDTO.getnodeid(), invokerequestsDTO.gettype());
    }
}
