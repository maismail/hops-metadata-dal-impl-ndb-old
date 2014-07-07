/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopDelegationKey;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.DelegationKeyDataAccess;
import se.sics.hop.metadata.yarn.tabledef.DelegationKeyTableDef;

/**
 *
 * @author nickstanogias
 */
public class DelegationKeyClusterJ implements DelegationKeyTableDef, DelegationKeyDataAccess<HopDelegationKey>{
  
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
    
    private HopDelegationKey createHopDelegationKey(DelegationKeyDTO delegationKeyDTO) {
        return new HopDelegationKey(delegationKeyDTO.getkey(), delegationKeyDTO.getdelegationkey());
    }

    private DelegationKeyDTO createPersistable(HopDelegationKey hop, Session session) {
        DelegationKeyClusterJ.DelegationKeyDTO delegationKeyDTO = session.newInstance(DelegationKeyClusterJ.DelegationKeyDTO.class);
        delegationKeyDTO.setkey(hop.getKey());
        delegationKeyDTO.setdelegationkey(hop.getDelegationkey());
        
        return delegationKeyDTO;
    }
}
