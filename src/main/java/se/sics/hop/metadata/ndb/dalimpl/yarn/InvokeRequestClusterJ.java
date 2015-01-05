package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.InvokeRequest;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.wrapper.HopsPredicate;
import se.sics.hop.metadata.ndb.wrapper.HopsQuery;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryBuilder;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryDomainType;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;
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
        HopsSession session = connector.obtainSession();
        session.makePersistent(createPersistable(invokeRequest, session));
    }

    @Override
    public InvokeRequest findByNodeId(int rmNodeId) throws StorageException {
        HopsSession session = connector.obtainSession();
        InvokeRequestDTO req = session.find(InvokeRequestDTO.class, rmNodeId);
        if (req == null) {
            throw new StorageException("Error while retrieving invoke request row:" + rmNodeId);
        }

        return createHopInvokeRequest(req);
    }

    @Override
    public List<InvokeRequest> findAll(int numberOfRequests, boolean pending) throws StorageException {
        HopsSession session = connector.obtainSession();
        HopsQueryBuilder qb = session.getQueryBuilder();
        HopsQueryDomainType<InvokeRequestDTO> dobj = qb.createQueryDefinition(InvokeRequestDTO.class);
        HopsPredicate pred1 = dobj.get(STATUS).equal(dobj.param(STATUS));
        dobj.where(pred1);
        HopsQuery<InvokeRequestDTO> query = session.createQuery(dobj);
        if (pending) {
            query.setParameter(STATUS, STATUS_PENDING);
        } else {
            query.setParameter(STATUS, STATUS_NEW);
        }
        List<InvokeRequestDTO> results = query.getResultList();
        try {
            return createHopInvokeRequestsList(results, numberOfRequests);
        } catch (IOException ex) {
            Logger.getLogger(InvokeRequestClusterJ.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    @Override
    public void prepare(Collection<InvokeRequest> modified, Collection<InvokeRequest> removed) throws StorageException {
        HopsSession session = connector.obtainSession();
        try {
            if (removed != null) {
                List<InvokeRequestDTO> toRemove = new ArrayList<InvokeRequestDTO>();
                for (InvokeRequest req : removed) {
                    toRemove.add(session.newInstance(InvokeRequestDTO.class, req.getNodeid()));
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                List<InvokeRequestDTO> toModify = new ArrayList<InvokeRequestDTO>();
                for (InvokeRequest req : modified) {
                    toModify.add(createPersistable(req, session));
                }
                session.savePersistentAll(toModify);
            }
        } catch (Exception e) {
            throw new StorageException("Error while modifying invokerequests, error:" + e.getMessage());
        }
    }

    private InvokeRequestDTO createPersistable(InvokeRequest hopInvokeRequests, HopsSession session) throws StorageException {
        InvokeRequestDTO invokerequestsDTO = session.newInstance(InvokeRequestDTO.class);
        invokerequestsDTO.setnodeid(hopInvokeRequests.getNodeid());
        invokerequestsDTO.settype(hopInvokeRequests.getType());
        invokerequestsDTO.setstatus(hopInvokeRequests.getStatus());
        //session.savePersistent(invokerequestsDTO);
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
