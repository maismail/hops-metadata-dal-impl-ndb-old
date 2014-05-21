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
        @Column(name = NODEID)
        int getnodeid();

        void setnodeid(int nodeid);

        @Column(name = TYPE)
        int gettype();

        void settype(int type);

        @Column(name = STATUS)
        int getstatus();

        void setstatus(int status);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public void createInvokeRequest(InvokeRequest invokeRequest) throws StorageException {
        Session session = connector.obtainSession();
        createPersistable(invokeRequest, session);
    }

    @Override
    public InvokeRequest findByNodeId(int rmNodeId) throws StorageException {
        Session session = connector.obtainSession();
        InvokeRequestDTO req = session.find(InvokeRequestDTO.class, rmNodeId);
        if (req == null) {
            throw new StorageException("Error while retrieving row:" + rmNodeId);
        }

        return createHopInvokeRequest(req);
    }

    @Override
    public List<InvokeRequest> findAll(int numberOfRequests, boolean pending) throws StorageException {
        Session session = connector.obtainSession();
        QueryBuilder qb = session.getQueryBuilder();
        QueryDomainType<InvokeRequestDTO> dobj = qb.createQueryDefinition(InvokeRequestDTO.class);
        Predicate pred1 = dobj.get(STATUS).equal(dobj.param(STATUS));
        dobj.where(pred1);
        Query<InvokeRequestDTO> query = session.createQuery(dobj);
        if (pending) {
            query.setParameter(STATUS, STATUS_PENDING);
        } else {
            query.setParameter(STATUS, STATUS_NEW);
        }
        List<InvokeRequestDTO> results = query.getResultList();
        try {
            return createHopInvokeRequestsList(results, numberOfRequests);
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
                    InvokeRequestDTO persistable = session.newInstance(InvokeRequestDTO.class, hopInvokeRequests.getNodeid());
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

    private InvokeRequestDTO createPersistable(InvokeRequest hopInvokeRequests, Session session) {
        InvokeRequestDTO invokerequestsDTO = session.newInstance(InvokeRequestDTO.class);
        invokerequestsDTO.setnodeid(hopInvokeRequests.getNodeid());
        invokerequestsDTO.settype(hopInvokeRequests.getType());
        invokerequestsDTO.setstatus(hopInvokeRequests.getStatus());
        session.savePersistent(invokerequestsDTO);
        return invokerequestsDTO;
    }

    private List<InvokeRequest> createHopInvokeRequestsList(List<InvokeRequestDTO> list) throws IOException {
        List<InvokeRequest> hopInvokeRequests = new ArrayList<InvokeRequest>();
        for (InvokeRequestDTO persistable : list) {
            hopInvokeRequests.add(createHopInvokeRequest(persistable));
        }
        return hopInvokeRequests;
    }

    private List<InvokeRequest> createHopInvokeRequestsList(List<InvokeRequestDTO> list, int numberOfRequests) throws IOException {
        List<InvokeRequest> hopInvokeRequests = new ArrayList<InvokeRequest>();
        int counter = 0;
        for (InvokeRequestDTO persistable : list) {
            if (numberOfRequests > 0 && counter == numberOfRequests) {
                break;
            }
            hopInvokeRequests.add(createHopInvokeRequest(persistable));
            counter++;
        }
        return hopInvokeRequests;
    }

    private InvokeRequest createHopInvokeRequest(InvokeRequestDTO invokerequestsDTO) {
        return new InvokeRequest(invokerequestsDTO.getnodeid(), invokerequestsDTO.gettype(), invokerequestsDTO.getstatus());
    }
}
