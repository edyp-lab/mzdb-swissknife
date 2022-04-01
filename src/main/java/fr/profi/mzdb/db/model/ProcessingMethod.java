package fr.profi.mzdb.db.model;

import fr.profi.mzdb.db.model.params.ParamTree;

public class ProcessingMethod extends AbstractTableModel {

  protected int number;

  protected String dataProcessingName;

  protected Integer softwareId;

  /**
   * Instantiates a new abstract table model.
   *
   * @param id        the id
   * @param paramTree
   */
  public ProcessingMethod(long id, ParamTree paramTree, int number, String dataProcessingName, Integer softwareId) {
    super(id, paramTree);
    this.number = number;
    this.dataProcessingName = dataProcessingName;
    this.softwareId = softwareId;
  }

  public int getNumber() {
    return number;
  }

  public void setNumber(int number) {
    this.number = number;
  }

  public String getDataProcessingName() {
    return dataProcessingName;
  }

  public void setDataProcessingName(String dataProcessingName) {
    this.dataProcessingName = dataProcessingName;
  }

  public Integer getSoftwareId() {
    return softwareId;
  }

  public void setSoftwareId(Integer softwareId) {
    this.softwareId = softwareId;
  }

}
