package io.hops.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.common.entity.Variable;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.VariableDataAccess;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class VariableClusterj
    implements TablesDef.VariableTableDef, VariableDataAccess<Variable, Variable.Finder> {

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
  public Variable getVariable(Variable.Finder varType) throws StorageException {
    HopsSession session = connector.obtainSession();
    VariableDTO var = session.find(VariableDTO.class, varType.getId());
    if (var == null) {
      throw new StorageException(
          "There is no variable entry with id " + varType.getId());
    }
    return Variable.initVariable(varType, var.getValue());
  }

  @Override
  public void setVariable(Variable var) throws StorageException {
    HopsSession session = connector.obtainSession();
    VariableDTO vd = createVariableDTO(session, var);
    session.savePersistent(vd);
  }

  @Override
  public void prepare(Collection<Variable> newVariables,
      Collection<Variable> updatedVariables,
      Collection<Variable> removedVariables) throws StorageException {
    HopsSession session = connector.obtainSession();
    removeVariables(session, removedVariables);
    updateVariables(session, newVariables);
    updateVariables(session, updatedVariables);
  }

  private void removeVariables(HopsSession session, Collection<Variable> vars)
      throws StorageException {
    if (vars != null) {
      List<VariableDTO> removed = new ArrayList<VariableDTO>();
      for (Variable var : vars) {
        VariableDTO vd =
            session.newInstance(VariableDTO.class, var.getType().getId());
        removed.add(vd);
      }
      session.deletePersistentAll(removed);
    }
  }

  private void updateVariables(HopsSession session, Collection<Variable> vars)
      throws StorageException {
    List<VariableDTO> changes = new ArrayList<VariableDTO>();
    for (Variable var : vars) {
      changes.add(createVariableDTO(session, var));
    }
    session.savePersistentAll(changes);
  }

  private VariableDTO createVariableDTO(HopsSession session, Variable var)
      throws StorageException {
    byte[] varVal = var.getBytes();
    if (varVal.length > MAX_VARIABLE_SIZE) {
      throw new StorageException("wrong variable size" + varVal.length +
          ", variable size should be less or equal to " + MAX_VARIABLE_SIZE);
    }
    VariableDTO vd = session.newInstance(VariableDTO.class);
    vd.setValue(var.getBytes());
    vd.setId(var.getType().getId());
    return vd;
  }
}
