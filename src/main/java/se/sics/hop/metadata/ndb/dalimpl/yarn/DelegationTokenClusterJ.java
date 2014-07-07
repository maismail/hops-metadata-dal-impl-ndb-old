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
import se.sics.hop.metadata.hdfs.entity.yarn.HopDelegationToken;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.DelegationTokenDataAccess;
import se.sics.hop.metadata.yarn.tabledef.DelegationTokenTableDef;

/**
 *
 * @author nickstanogias
 */
public class DelegationTokenClusterJ implements DelegationTokenTableDef, DelegationTokenDataAccess<HopDelegationToken>{ 

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

        DelegationTokenClusterJ.DelegationTokenDTO delegationTokenDTO = null;
        if (session != null) {
            delegationTokenDTO = session.find(DelegationTokenClusterJ.DelegationTokenDTO.class, seqnumber);
        }
        if (delegationTokenDTO == null) {
                throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopDelegationToken(delegationTokenDTO);
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

    private DelegationTokenDTO createPersistable(HopDelegationToken hop, Session session) {
        DelegationTokenClusterJ.DelegationTokenDTO delegationTokenDTO = session.newInstance(DelegationTokenClusterJ.DelegationTokenDTO.class);
        delegationTokenDTO.setseqnumber(hop.getSeqnumber());
        delegationTokenDTO.setrmdtidentifier(hop.getRmdtidentifier());
        
        return delegationTokenDTO;
    }    
}
