package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopPendingEvent;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.wrapper.HopsPredicate;
import se.sics.hop.metadata.ndb.wrapper.HopsQuery;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryBuilder;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryDomainType;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;
import se.sics.hop.metadata.yarn.dal.PendingEventDataAccess;
import se.sics.hop.metadata.yarn.tabledef.PendingEventTableDef;

/**
 * Implements persistence of PersistedEvents to NDB to be retrieved
 * by the scheduler.
 * <p>
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class PendingEventClusterJ implements PendingEventTableDef,
        PendingEventDataAccess<HopPendingEvent> {

  private static final Log LOG = LogFactory.getLog(PendingEventClusterJ.class);

  @PersistenceCapable(table = TABLE_NAME)
  public interface PendingEventDTO {

    @PrimaryKey
    @Column(name = ID)
    int getId();

    void setId(int id);

    @PrimaryKey
    @Column(name = RMNODEID)
    String getrmnodeid();

    void setrmnodeid(String rmnodeid);

    @Column(name = TYPE)
    byte getType();

    void setType(byte type);

    @Column(name = STATUS)
    byte getStatus();

    void setStatus(byte status);

  }
  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public void createPendingEvent(HopPendingEvent persistedEvent) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    session.makePersistent(createPersistable(persistedEvent, session));
  }

  @Override
  public void removePendingEvent(HopPendingEvent persistedEvent) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    session.deletePersistent(createPersistable(persistedEvent, session));
  }

  @Override
  public void prepare(Collection<HopPendingEvent> modified,
          Collection<HopPendingEvent> removed) throws StorageException {
    HopsSession session = connector.obtainSession();
    if (removed != null && !removed.isEmpty()) {
      LOG.debug("HOP :: ClusterJ PendingEvent.prepare.remove - START:"
              + removed);
      List<PendingEventDTO> toRemove = new ArrayList<PendingEventDTO>();
      for (HopPendingEvent hop : removed) {
        toRemove.add(session.newInstance(PendingEventDTO.class, new Object[]{hop.getId(),hop.
                getRmnodeId()}));
      }
      session.deletePersistentAll(toRemove);
      LOG.debug("HOP :: ClusterJ PendingEvent.prepare.remove - FINISH:"
              + removed);
    }
    if (modified != null && !modified.isEmpty()) {
      LOG.debug("HOP :: ClusterJ PendingEvent.prepare.modify - START:"
              + modified);
      List<PendingEventDTO> toModify = new ArrayList<PendingEventDTO>();
      for (HopPendingEvent hop : modified) {
        toModify.add(createPersistable(hop, session));
      }
      session.savePersistentAll(toModify);
      LOG.debug("HOP :: ClusterJ PendingEvent.prepare.modify - FINISH:"
              + modified);
    }
    session.flush();
  }

  @Override
  public List<HopPendingEvent> getAll() throws StorageException {
    LOG.debug("HOP :: ClusterJ PendingEvent.getAll - START");
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<PendingEventDTO> dobj = qb.createQueryDefinition(
            PendingEventDTO.class);
    HopsQuery<PendingEventDTO> query = session.createQuery(dobj);

    List<PendingEventDTO> results = query.getResultList();
    LOG.debug("HOP :: ClusterJ PendingEvent.getAll - FINISH");
    return createPendingEventList(results);
  }

  @Override
  public List<HopPendingEvent> getAll(byte status) throws StorageException {
    // LOG.debug("HOP :: ClusterJ PendingEvent.getAll(" + status + ") - START");
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<PendingEventDTO> dobj = qb.createQueryDefinition(
            PendingEventDTO.class);
    HopsPredicate pred1 = dobj.get(STATUS).equal(dobj.param(STATUS));
    dobj.where(pred1);
    HopsQuery<PendingEventDTO> query = session.createQuery(dobj);
    query.setParameter(STATUS, status);
    List<PendingEventDTO> results = query.getResultList();
    //LOG.debug("HOP :: ClusterJ PendingEvent.getAll(" + status + ") - FINISH");
    return createPendingEventList(results);
  }

  /**
   *
   * <p>
   * @param hopPersistedEvent
   * @param session
   * @return
   */
  private PendingEventDTO createPersistable(
          HopPendingEvent hopPersistedEvent, HopsSession session) throws
          StorageException {
    PendingEventDTO DTO = session.newInstance(PendingEventDTO.class);
    //Set values to persist new persistedEvent
    DTO.setrmnodeid(hopPersistedEvent.getRmnodeId());
    DTO.setType(hopPersistedEvent.getType());
    DTO.setStatus(hopPersistedEvent.getStatus());
    DTO.setId(hopPersistedEvent.getId());
    return DTO;
  }

  /**
   * Create a list with HOP objects from DTO.
   * <p>
   * @param results
   * @return
   */
  private List<HopPendingEvent> createPendingEventList(
          List<PendingEventDTO> results) {
    List<HopPendingEvent> hopList = null;
    if (results != null && !results.isEmpty()) {
      hopList = new ArrayList<HopPendingEvent>(results.size());
      for (PendingEventDTO DTO : results) {
        HopPendingEvent hop = new HopPendingEvent(DTO.getrmnodeid(), DTO.
                getType(), DTO.getStatus(), DTO.getId());
        hopList.add(hop);
      }
    }
    return hopList;
  }
}
