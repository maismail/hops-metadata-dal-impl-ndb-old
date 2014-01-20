package se.sics.hop.metadata.persistence.ndb.dalimpl;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.Collection;
import se.sics.hop.metadata.persistence.entity.hop.var.HopVariable;
import se.sics.hop.metadata.persistence.dal.VariableDataAccess;
import se.sics.hop.metadata.persistence.exceptions.StorageException;
import se.sics.hop.metadata.persistence.ndb.ClusterjConnector;
import se.sics.hop.metadata.persistence.tabledef.VariableTableDef;

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
      Session session = connector.obtainSession();
      VariableDTO var = session.find(VariableDTO.class, varType.getId());
      if (var == null) {
        throw new StorageException("There is no variable entry with id " + varType.getId());
      }
      return HopVariable.initVariable(varType, var.getValue());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void prepare(Collection<HopVariable> newVariables, Collection<HopVariable> updatedVariables, Collection<HopVariable> removedVariables) throws StorageException {
    try {
      Session session = connector.obtainSession();
      removeVariables(session, removedVariables);
      updateVariables(session, newVariables);
      updateVariables(session, updatedVariables);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private void removeVariables(Session session, Collection<HopVariable> vars) {
    if (vars != null) {
      for (HopVariable var : vars) {
        VariableDTO vd = session.newInstance(VariableDTO.class, var.getType().getId());
        session.deletePersistent(vd);
      }
    }
  }

  private void updateVariables(Session session, Collection<HopVariable> vars) throws StorageException {
    for (HopVariable var : vars) {
      byte[] varVal = var.getBytes();
      if (varVal.length > MAX_VARIABLE_SIZE) {
        throw new StorageException("wrong variable size" + varVal.length + ", variable size should be less or equal to " + MAX_VARIABLE_SIZE);
      }
      VariableDTO vd = session.newInstance(VariableDTO.class);
      vd.setValue(var.getBytes());
      vd.setId(var.getType().getId());
      session.savePersistent(vd);
    }
  }
}
