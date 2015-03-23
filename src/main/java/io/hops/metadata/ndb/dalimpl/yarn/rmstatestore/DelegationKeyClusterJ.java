package io.hops.metadata.ndb.dalimpl.yarn.rmstatestore;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.DataFormatException;

import io.hops.exception.StorageException;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.util.CompressionUtils;
import io.hops.metadata.yarn.entity.rmstatestore.DelegationKey;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.yarn.dal.rmstatestore.DelegationKeyDataAccess;
import io.hops.metadata.yarn.tabledef.rmstatestore.DelegationKeyTableDef;

public class DelegationKeyClusterJ implements DelegationKeyTableDef, DelegationKeyDataAccess<DelegationKey> {

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
  public void remove(DelegationKey removed) throws StorageException {
    HopsSession session = connector.obtainSession();
    session.deletePersistent(session.newInstance(
            DelegationKeyClusterJ.DelegationKeyDTO.class, removed.getKey()));
  }

    @Override
    public void createDTMasterKeyEntry(DelegationKey hopDelegationKey) throws StorageException{
        HopsSession session = connector.obtainSession();
        session.savePersistent(createPersistable(hopDelegationKey, session));
    }

    @Override
    public List<DelegationKey> getAll() throws StorageException {
        try {
            HopsSession session = connector.obtainSession();
            HopsQueryBuilder qb = session.getQueryBuilder();
            HopsQueryDomainType<DelegationKeyDTO> dobj = qb.createQueryDefinition(DelegationKeyDTO.class);
            HopsQuery<DelegationKeyDTO> query = session.createQuery(dobj);
            List<DelegationKeyDTO> results = query.getResultList();
            
                return createHopDelegationKeyList(results);
           
        } catch (Exception e) {
            throw new StorageException(e);
        }

    }

  private DelegationKey createHopDelegationKey(
          DelegationKeyDTO delegationKeyDTO)
          throws StorageException {
    try {
      return new DelegationKey(delegationKeyDTO.getkey(), CompressionUtils.
              decompress(delegationKeyDTO.getdelegationkey()));
    } catch (IOException e) {
      throw new StorageException(e);
    } catch (DataFormatException e) {
      throw new StorageException(e);
    }
  }

    private List<DelegationKey> createHopDelegationKeyList(List<DelegationKeyClusterJ.DelegationKeyDTO> list) throws StorageException {
        List<DelegationKey> hopList = new ArrayList<DelegationKey>();
        for (DelegationKeyClusterJ.DelegationKeyDTO dto : list) {
            hopList.add(createHopDelegationKey(dto));
        }
        return hopList;
    }

  private DelegationKeyDTO createPersistable(DelegationKey hop,
          HopsSession session) throws StorageException {
    DelegationKeyClusterJ.DelegationKeyDTO delegationKeyDTO = session.
            newInstance(DelegationKeyClusterJ.DelegationKeyDTO.class);
    delegationKeyDTO.setkey(hop.getKey());
    try {
      delegationKeyDTO.setdelegationkey(CompressionUtils.compress(hop.
              getDelegationkey()));
    } catch (IOException e) {
      throw new StorageException(e);
    }

    return delegationKeyDTO;
  }
}
