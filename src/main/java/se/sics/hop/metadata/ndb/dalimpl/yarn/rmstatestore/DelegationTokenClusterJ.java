package se.sics.hop.metadata.ndb.dalimpl.yarn.rmstatestore;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.rmstatestore.HopDelegationToken;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.DelegationTokenDataAccess;
import se.sics.hop.metadata.yarn.tabledef.rmstatestore.DelegationTokenTableDef;

/**
 *
 * @author nickstanogias
 */
public class DelegationTokenClusterJ implements DelegationTokenTableDef, DelegationTokenDataAccess<HopDelegationToken> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface DelegationTokenDTO {

        @PrimaryKey
        @Column(name = SEQ_NUMBER)
        int getseqnumber();

        void setseqnumber(int seqnumber);

        @Column(name = RMDT_IDENTIFIER)
        byte[] getrmdtidentifier();

        void setrmdtidentifier(byte[] rmdtidentifier);
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopDelegationToken findBySeqNumber(int seqnumber) throws StorageException {
        Session session = connector.obtainSession();

        DelegationTokenDTO delegationTokenDTO = null;
        if (session != null) {
            delegationTokenDTO = session.find(DelegationTokenDTO.class, seqnumber);
        }
        if (delegationTokenDTO == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopDelegationToken(delegationTokenDTO);
    }

    @Override
    public void createDelegationTokenEntry(HopDelegationToken hopDelegationToken) {
        Session session = connector.obtainSession();
        session.savePersistent(createPersistable(hopDelegationToken, session));
    }

    @Override
    public List<HopDelegationToken> getAll() throws StorageException {
        try {
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();
            QueryDomainType<DelegationTokenDTO> dobj = qb.createQueryDefinition(DelegationTokenDTO.class);
            Query<DelegationTokenDTO> query = session.createQuery(dobj);
            List<DelegationTokenDTO> results = query.getResultList();
            if (results != null && !results.isEmpty()) {
                return createHopDelegationTokenList(results);
            } else {
                throw new StorageException("HOP :: Error retrieving DelegationTokens");
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }

    }

    @Override
    public void prepare(Collection<HopDelegationToken> modified, Collection<HopDelegationToken> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                List<DelegationTokenDTO> toRemove = new ArrayList<DelegationTokenDTO>();
                for (HopDelegationToken hop : removed) {
                    toRemove.add(session.newInstance(DelegationTokenClusterJ.DelegationTokenDTO.class, hop.getSeqnumber()));
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                List<DelegationTokenDTO> toModify = new ArrayList<DelegationTokenDTO>();
                for (HopDelegationToken hop : modified) {
                    toModify.add(createPersistable(hop, session));
                }
                session.savePersistentAll(toModify);
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    private HopDelegationToken createHopDelegationToken(DelegationTokenDTO delegationTokenDTO) {
        return new HopDelegationToken(delegationTokenDTO.getseqnumber(), delegationTokenDTO.getrmdtidentifier());
    }

    private List<HopDelegationToken> createHopDelegationTokenList(List<DelegationTokenClusterJ.DelegationTokenDTO> list) {
        List<HopDelegationToken> hopList = new ArrayList<HopDelegationToken>();
        for (DelegationTokenClusterJ.DelegationTokenDTO dto : list) {
            hopList.add(createHopDelegationToken(dto));
        }
        return hopList;
    }

    private DelegationTokenDTO createPersistable(HopDelegationToken hop, Session session) {
        DelegationTokenClusterJ.DelegationTokenDTO delegationTokenDTO = session.newInstance(DelegationTokenClusterJ.DelegationTokenDTO.class);
        delegationTokenDTO.setseqnumber(hop.getSeqnumber());
        delegationTokenDTO.setrmdtidentifier(hop.getRmdtidentifier());

        return delegationTokenDTO;
    }
}
