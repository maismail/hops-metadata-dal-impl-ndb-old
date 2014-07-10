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
import se.sics.hop.metadata.hdfs.entity.yarn.rmstatestore.HopDelegationKey;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.DelegationKeyDataAccess;
import se.sics.hop.metadata.yarn.tabledef.rmstatestore.DelegationKeyTableDef;

/**
 *
 * @author nickstanogias
 */
public class DelegationKeyClusterJ implements DelegationKeyTableDef, DelegationKeyDataAccess<HopDelegationKey> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface DelegationKeyDTO {

        @PrimaryKey
        @Column(name = KEY)
        int getkey();

        void setkey(int key);

        @Column(name = DELEGATIONKEY)
        byte[] getdelegationkey();

        void setdelegationkey(byte[] delegationkey);
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopDelegationKey findByKey(int key) throws StorageException {
        Session session = connector.obtainSession();

        DelegationKeyClusterJ.DelegationKeyDTO delegationKeyDTO = null;
        if (session != null) {
            delegationKeyDTO = session.find(DelegationKeyClusterJ.DelegationKeyDTO.class, key);
        }
        if (delegationKeyDTO == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopDelegationKey(delegationKeyDTO);
    }

    @Override
    public void prepare(Collection<HopDelegationKey> modified, Collection<HopDelegationKey> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                List<DelegationKeyDTO> toRemove = new ArrayList<DelegationKeyDTO>();
                for (HopDelegationKey hop : removed) {
                    toRemove.add(session.newInstance(DelegationKeyClusterJ.DelegationKeyDTO.class, hop.getKey()));
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                List<DelegationKeyDTO> toModify = new ArrayList<DelegationKeyDTO>();
                for (HopDelegationKey hop : modified) {
                    toModify.add(createPersistable(hop, session));
                }
                session.savePersistentAll(toModify);
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void createDTMasterKeyEntry(HopDelegationKey hopDelegationKey) {
        Session session = connector.obtainSession();
        session.savePersistent(createPersistable(hopDelegationKey, session));
    }

    @Override
    public List<HopDelegationKey> getAll() throws StorageException {
        try {
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();
            QueryDomainType<DelegationKeyDTO> dobj = qb.createQueryDefinition(DelegationKeyDTO.class);
            Query<DelegationKeyDTO> query = session.createQuery(dobj);
            List<DelegationKeyDTO> results = query.getResultList();
            if (results != null && !results.isEmpty()) {
                return createHopDelegationKeyList(results);
            } else {
                throw new StorageException("HOP :: Error retrieving DelegationKeys");
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }

    }

    private HopDelegationKey createHopDelegationKey(DelegationKeyDTO delegationKeyDTO) {
        return new HopDelegationKey(delegationKeyDTO.getkey(), delegationKeyDTO.getdelegationkey());
    }

    private List<HopDelegationKey> createHopDelegationKeyList(List<DelegationKeyClusterJ.DelegationKeyDTO> list) {
        List<HopDelegationKey> hopList = new ArrayList<HopDelegationKey>();
        for (DelegationKeyClusterJ.DelegationKeyDTO dto : list) {
            hopList.add(createHopDelegationKey(dto));
        }
        return hopList;
    }

    private DelegationKeyDTO createPersistable(HopDelegationKey hop, Session session) {
        DelegationKeyClusterJ.DelegationKeyDTO delegationKeyDTO = session.newInstance(DelegationKeyClusterJ.DelegationKeyDTO.class);
        delegationKeyDTO.setkey(hop.getKey());
        delegationKeyDTO.setdelegationkey(hop.getDelegationkey());

        return delegationKeyDTO;
    }
}
