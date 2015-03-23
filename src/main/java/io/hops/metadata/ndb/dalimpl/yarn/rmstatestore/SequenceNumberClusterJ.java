package io.hops.metadata.ndb.dalimpl.yarn.rmstatestore;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;

import io.hops.exception.StorageException;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.dal.rmstatestore.SequenceNumberDataAccess;
import io.hops.metadata.yarn.tabledef.rmstatestore.SequenceNumberTableDef;
import io.hops.metadata.yarn.entity.rmstatestore.SequenceNumber;
import io.hops.metadata.ndb.ClusterjConnector;

public class SequenceNumberClusterJ implements SequenceNumberTableDef,
    SequenceNumberDataAccess<SequenceNumber> {

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
    public SequenceNumber findById(int id) throws StorageException {
        HopsSession session = connector.obtainSession();

        SequenceNumberDTO sequenceNumberDTO = null;
        if (session != null) {
            sequenceNumberDTO = session.find(SequenceNumberDTO.class, id);
        }
        

        return createHopSequenceNumber(sequenceNumberDTO);
    }

  @Override
  public void add(SequenceNumber toAdd) throws StorageException {
    HopsSession session = connector.obtainSession();
    session.savePersistent(createPersistable(toAdd, session));
  }

    private SequenceNumber createHopSequenceNumber(SequenceNumberDTO sequenceNumberDTO) {
      if(sequenceNumberDTO !=null){
        return new SequenceNumber(sequenceNumberDTO.getid(), sequenceNumberDTO.getsequencenumber());
      } else{
        return null;
      }
    }

    private SequenceNumberDTO createPersistable(SequenceNumber hop, HopsSession session) throws StorageException {
        SequenceNumberDTO sequenceNumberDTO = session.newInstance(SequenceNumberDTO.class);
        sequenceNumberDTO.setid(hop.getId());
        sequenceNumberDTO.setsequencenumber(hop.getSequencenumber());

        return sequenceNumberDTO;
    }
}
