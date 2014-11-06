package se.sics.hop.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import se.sics.hop.metadata.hdfs.entity.hop.var.HopVariable;
import se.sics.hop.metadata.hdfs.dal.VariableDataAccess;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.hdfs.tabledef.VariableTableDef;
import se.sics.hop.metadata.ndb.DBSession;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class VariableClusterj implements VariableTableDef, VariableDataAccess<HopVariable, HopVariable.Finder> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface VariableDTO {

    @PrimaryKey
    @Column(name = ID)
    int getId();

    void setId(int id);

    @Column(name = VARIABLE_VALUE)
    byte[] getValue();

    void setValue(byte[] value);
  }
  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public HopVariable getVariable(HopVariable.Finder varType) throws StorageException {
    try {
      DBSession dbSession = connector.obtainSession();
      VariableDTO var = dbSession.getSession().find(VariableDTO.class, varType.getId());
      if (var == null) {
        throw new StorageException("There is no variable entry with id " + varType.getId());
      }
      return HopVariable.initVariable(varType, var.getValue());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void setVariable(HopVariable var) throws StorageException {
    try {
      DBSession dbSession = connector.obtainSession();
      VariableDTO vd = createVariableDTO(dbSession, var);
      dbSession.getSession().savePersistent(vd);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }
    
  @Override
  public void prepare(Collection<HopVariable> newVariables, Collection<HopVariable> updatedVariables, Collection<HopVariable> removedVariables) throws StorageException {
    try {
      DBSession dbSession = connector.obtainSession();
      removeVariables(dbSession, removedVariables);
      updateVariables(dbSession, newVariables);
      updateVariables(dbSession, updatedVariables);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private void removeVariables(DBSession dbSession, Collection<HopVariable> vars) {
    if (vars != null) {
      List<VariableDTO> removed = new ArrayList<VariableDTO>();
      for (HopVariable var : vars) {
        VariableDTO vd = dbSession.getSession().newInstance(VariableDTO.class, var.getType().getId());
        removed.add(vd);
      }
      dbSession.getSession().deletePersistentAll(removed);
    }
  }

  private void updateVariables(DBSession dbSession, Collection<HopVariable> vars) throws StorageException {
    List<VariableDTO> changes= new ArrayList<VariableDTO>();
    for (HopVariable var : vars) {
      changes.add(createVariableDTO(dbSession, var));
    }
    dbSession.getSession().savePersistentAll(changes);
  }

  private VariableDTO createVariableDTO(DBSession dbSession, HopVariable var) throws StorageException {
    byte[] varVal = var.getBytes();
    if (varVal.length > MAX_VARIABLE_SIZE) {
      throw new StorageException("wrong variable size" + varVal.length + ", variable size should be less or equal to " + MAX_VARIABLE_SIZE);
    }
    VariableDTO vd = dbSession.getSession().newInstance(VariableDTO.class);
    vd.setValue(var.getBytes());
    vd.setId(var.getType().getId());
    return vd;
  }
}
