package fr.profi.mzdb.db.model;

import fr.profi.mzdb.db.model.params.param.CV;
import fr.profi.mzdb.db.model.params.param.CVTerm;
import fr.profi.mzdb.db.model.params.param.CVUnit;
import fr.profi.mzdb.model.DataEncoding;

import java.util.ArrayList;
import java.util.List;

public class MzDBMetaData {

  protected MzDbHeader mzdbHeader;
  protected List<DataEncoding> dataEncodings = new ArrayList<>();
  protected List<InstrumentConfiguration> instrumentConfigurations= new ArrayList<>();
  protected List<SharedParamTree> sharedParamTrees = new ArrayList<>();
  protected List<ProcessingMethod> processingMethods = new ArrayList<>();
  protected List<Run> runs= new ArrayList<>();
  protected List<Sample> samples= new ArrayList<>();
  protected List<Software> softwares= new ArrayList<>();
  protected List<SourceFile> sourceFiles= new ArrayList<>();
  protected List<CV> cvList = new ArrayList<>();
  protected List<CVTerm> cvTerms = new ArrayList<>();
  protected List<CVUnit> cvUnits = new ArrayList<>();


  public MzDBMetaData(){
    //Init list as empty
  }

  public MzDbHeader getMzdbHeader() {
    return mzdbHeader;
  }

  public void setMzdbHeader(MzDbHeader mzdbHeader) {
    this.mzdbHeader = mzdbHeader;
  }

  public List<DataEncoding> getDataEncodings() {
    return dataEncodings;
  }

  public void setDataEncodings(List<DataEncoding> dataEncodings) {
    this.dataEncodings = dataEncodings;
  }

  public List<InstrumentConfiguration> getInstrumentConfigurations() {
    return instrumentConfigurations;
  }

  public void setInstrumentConfigurations(List<InstrumentConfiguration> instrumentConfigurations) {
    this.instrumentConfigurations = instrumentConfigurations;
  }

  public List<SharedParamTree>  getSharedParamTrees() {
    return sharedParamTrees;
  }

  public void setSharedParamTrees(List<SharedParamTree> sharedParamTrees) {
    this.sharedParamTrees = sharedParamTrees;
  }

  public List<ProcessingMethod> getProcessingMethods() {
    return processingMethods;
  }

  public void setProcessingMethods(List<ProcessingMethod> processingMethods) {
    this.processingMethods = processingMethods;
  }

  public List<Run> getRuns() {
    return runs;
  }

  public void setRuns(List<Run> runs) {
    this.runs = runs;
  }

  public List<Sample> getSamples() {
    return samples;
  }

  public void setSamples(List<Sample> samples) {
    this.samples = samples;
  }

  public List<Software> getSoftwares() {
    return softwares;
  }

  public void setSoftwares(List<Software> softwares) {
    this.softwares = softwares;
  }

  public List<SourceFile> getSourceFiles() {
    return sourceFiles;
  }

  public void setSourceFiles(List<SourceFile> sourceFiles) {
    this.sourceFiles = sourceFiles;
  }

  public List<CV> getCvList() {
    return cvList;
  }

  public void setCvList(List<CV> cvList) {
    this.cvList = cvList;
  }

  public List<CVTerm> getCvTerms() {
    return cvTerms;
  }

  public void setCvTerms(List<CVTerm> cvTerms) {
    this.cvTerms = cvTerms;
  }

  public List<CVUnit> getCvUnits() {
    return cvUnits;
  }

  public void setCvUnits(List<CVUnit> cvUnits) {
    this.cvUnits = cvUnits;
  }
}
