package se.sics.hop.metadata.ndb.dalimpl.yarn.rmstatestore;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.rmstatestore.HopSequenceNumber;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;
import se.sics.hop.metadata.yarn.dal.rmstatestore.SequenceNumberDataAccess;
import se.sics.hop.metadata.yarn.tabledef.rmstatestore.SequenceNumberTableDef;

public class SequenceNumberClusterJ implements SequenceNumberTableDef, SequenceNumberDataAccess<HopSequenceNumber> {

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
        HopsSession session = connector.obtainSession();

        SequenceNumberDTO sequenceNumberDTO = null;
        if (session != null) {
            sequenceNumberDTO = session.find(SequenceNumberDTO.class, id);
        }
        

        return createHopSequenceNumber(sequenceNumberDTO);
    }

  @Override
  public void add(HopSequenceNumber toAdd) throws StorageException {
    HopsSession session = connector.obtainSession();
    session.savePersistent(createPersistable(toAdd, session));
  }

    private HopSequenceNumber createHopSequenceNumber(SequenceNumberDTO sequenceNumberDTO) {
      if(sequenceNumberDTO !=null){
        return new HopSequenceNumber(sequenceNumberDTO.getid(), sequenceNumberDTO.getsequencenumber());
      } else{
        return null;
      }
    }

    private SequenceNumberDTO createPersistable(HopSequenceNumber hop, HopsSession session) throws StorageException {
        SequenceNumberDTO sequenceNumberDTO = session.newInstance(SequenceNumberDTO.class);
        sequenceNumberDTO.setid(hop.getId());
        sequenceNumberDTO.setsequencenumber(hop.getSequencenumber());

        return sequenceNumberDTO;
    }
}
