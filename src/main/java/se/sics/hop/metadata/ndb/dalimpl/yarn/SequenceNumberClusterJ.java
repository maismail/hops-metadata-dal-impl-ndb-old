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
import se.sics.hop.metadata.hdfs.entity.yarn.HopSequenceNumber;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.SequenceNumberDataAccess;
import se.sics.hop.metadata.yarn.tabledef.SequenceNumberTableDef;

/**
 *
 * @author nickstanogias
 */
public class SequenceNumberClusterJ implements SequenceNumberTableDef, SequenceNumberDataAccess<HopSequenceNumber>{

    @PersistenceCapable(table = TABLE_NAME)
    public interface SequenceNumberDTO {

        @PrimaryKey
        @Column(name = ID)
        int getid();
        void setid(int id);

        @Column(name = SEQUENCE_NUMBER)
        int getsequencenumber();
        void setsequencenumber(int sequencenumber);
        
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();
    
    @Override
    public HopSequenceNumber findById(int id) throws StorageException {
        Session session = connector.obtainSession();

        SequenceNumberClusterJ.SequenceNumberDTO sequenceNumberDTO = null;
        if (session != null) {
            sequenceNumberDTO = session.find(SequenceNumberClusterJ.SequenceNumberDTO.class, id);
        }
        if (sequenceNumberDTO == null) {
                throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopSequenceNumber(sequenceNumberDTO);
    }

    @Override
    public void prepare(Collection<HopSequenceNumber> modified, Collection<HopSequenceNumber> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                List<SequenceNumberDTO> toRemove = new ArrayList<SequenceNumberDTO>();
                for (HopSequenceNumber hop : removed) {
                    toRemove.add(session.newInstance(SequenceNumberClusterJ.SequenceNumberDTO.class, hop.getId()));
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                List<SequenceNumberDTO> toModify = new ArrayList<SequenceNumberDTO>();
                for (HopSequenceNumber hop : modified) {
                    toModify.add(createPersistable(hop, session));
                }
                session.savePersistentAll(toModify);
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    private HopSequenceNumber createHopSequenceNumber(SequenceNumberDTO sequenceNumberDTO) {
        return new HopSequenceNumber(sequenceNumberDTO.getid(), sequenceNumberDTO.getsequencenumber());
    }

    private SequenceNumberDTO createPersistable(HopSequenceNumber hop, Session session) {
        SequenceNumberClusterJ.SequenceNumberDTO sequenceNumberDTO = session.newInstance(SequenceNumberClusterJ.SequenceNumberDTO.class);
        sequenceNumberDTO.setid(hop.getId());
        sequenceNumberDTO.setsequencenumber(hop.getSequencenumber());
        
        return sequenceNumberDTO;
    } 
}
