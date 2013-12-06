package se.sics.hop.metadata.persistence.ndb.dalimpl;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.PredicateOperand;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import se.sics.hop.metadata.persistence.dal.BlockTokenKeyDataAccess;
import se.sics.hop.metadata.persistence.exceptions.StorageException;
import se.sics.hop.metadata.persistence.entity.hdfs.HopBlockKey;
import se.sics.hop.metadata.persistence.ndb.ClusterjConnector;
import se.sics.hop.metadata.persistence.tabledef.BlockTokenTableDef;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class BlockTokenKeyClusterj implements BlockTokenTableDef, BlockTokenKeyDataAccess<HopBlockKey> {
  
  @Override
  public HopBlockKey findByKeyType(short keyType) throws StorageException {
    Session session = connector.obtainSession();
    QueryBuilder qb = session.getQueryBuilder();
    QueryDomainType<BlockKeyDTO> dobj = qb.createQueryDefinition(BlockKeyDTO.class);
    PredicateOperand field = dobj.get("keyType");
    Predicate predicate = field.equal(dobj.param("param1"));
    dobj.where(predicate);
    Query<BlockKeyDTO> query = session.createQuery(dobj);
    query.setParameter("param1", keyType);
    List<BlockKeyDTO> results = query.getResultList();
    if (results == null || results.isEmpty()) {
      return null;
    } else if (results.size() > 1) {
      throw new StorageException("More than 1 keys found for KeyType "
              + keyType + " - This should never happen or the world will end");
    } else {
      try {
        return createBlockKey(results.get(0));
      } catch (IOException e) {
        throw new StorageException(e);
      }
    }
  }
  
  @PersistenceCapable(table = TABLE_NAME)
  public interface BlockKeyDTO {
    
    @PrimaryKey
    @Column(name = KEY_ID)
    int getKeyId();
    
    void setKeyId(int keyId);
    
    @Column(name = EXPIRY_DATE)
    long getExpiryDate();
    
    void setExpiryDate(long expiryDate);
    
    @Column(name = KEY_BYTES)
    byte[] getKeyBytes();
    
    void setKeyBytes(byte[] keyBytes);
    
    @Column(name = KEY_TYPE)
    short getKeyType();
    
    void setKeyType(short keyType);
  }
  private ClusterjConnector connector = ClusterjConnector.getInstance();
  
  @Override
  public HopBlockKey findByKeyId(int keyId) throws StorageException {
    try {
      Session session = connector.obtainSession();
      BlockKeyDTO dk = session.find(BlockKeyDTO.class, keyId);
      if (dk == null) {
        return null;
      }
      return createBlockKey(dk);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }
  
  @Override
  public List<HopBlockKey> findAll() throws StorageException {
    List<HopBlockKey> blockKeys = new ArrayList<HopBlockKey>();
    try {
      Session session = connector.obtainSession();
      QueryBuilder qb = session.getQueryBuilder();
      QueryDomainType<BlockKeyDTO> dobj = qb.createQueryDefinition(BlockKeyDTO.class);
      Query<BlockKeyDTO> query = session.createQuery(dobj);
      List<BlockKeyDTO> storedKeys = query.getResultList();
      for (BlockKeyDTO key : storedKeys) {
        blockKeys.add(createBlockKey(key));
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
    return blockKeys;
  }
  
  @Override
  public void prepare(Collection<HopBlockKey> removed, Collection<HopBlockKey> newed, Collection<HopBlockKey> modified) throws StorageException {
    try {
      Session session = connector.obtainSession();
      for (HopBlockKey key : removed) {
        BlockKeyDTO kTable = session.newInstance(BlockKeyDTO.class, key.getKeyId());
        session.deletePersistent(kTable);
      }
      
      for (HopBlockKey key : newed) {
        BlockKeyDTO kTable = session.newInstance(BlockKeyDTO.class);
        createPersistable(key, kTable);
        session.savePersistent(kTable);
      }
      
      for (HopBlockKey key : modified) {
        BlockKeyDTO kTable = session.newInstance(BlockKeyDTO.class);
        createPersistable(key, kTable);
        session.savePersistent(kTable);
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }
  
  private HopBlockKey createBlockKey(BlockKeyDTO dk) throws IOException {
    HopBlockKey bKey = new HopBlockKey(dk.getKeyId(), dk.getExpiryDate(), dk.getKeyBytes(), dk.getKeyType());
    return bKey;
  }
  
  private void createPersistable(HopBlockKey key, BlockKeyDTO kTable) throws IOException {
    kTable.setExpiryDate(key.getExpiryDate());
    kTable.setKeyId(key.getKeyId());
    kTable.setKeyType(key.getKeyType());
    kTable.setKeyBytes(key.getKeyBytes());
  }
  
  @Override
  public void removeAll() throws StorageException {
    Session session = connector.obtainSession();
    session.deletePersistentAll(BlockKeyDTO.class);
  }
}
