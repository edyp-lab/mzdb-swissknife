package fr.profi.timstof;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import fr.profi.bruker.timstof.model.SpectrumGeneratingMethod;

public class ConverterArguments {


  public class MS1MethodConverter implements IStringConverter<SpectrumGeneratingMethod> {

    @Override
    public SpectrumGeneratingMethod convert(String s) {
      return SpectrumGeneratingMethod.valueOf(s.trim().toUpperCase());
    }

  }

  public class MobilityRepresentationMethodConverter implements IStringConverter<MobilityRepresentationMethod> {

    @Override
    public MobilityRepresentationMethod convert(String s) {
      return MobilityRepresentationMethod.valueOf(s.trim().toUpperCase());
    }

  }

  @Parameter(names = "-ms1_generation", description = "MS1 spectrum generation method. FULL: keep all peaks (for same moz, keep most intense); MERGED: group peaks by moz at 10ppm (keep most intense value); SMOOTH: smooth the spectra, detect max points and keep peaks for these points using original intensity", required = false, converter = MS1MethodConverter.class)
  SpectrumGeneratingMethod ms1 = SpectrumGeneratingMethod.SMOOTH;

  @Parameter(names = "-mobility", description = "Ion mobility representation method; NONE : generate a single spectrum for all ion mobility scans (the IM dimension is squashed); PER_SCAN : create one spectrum per scan of ion mobility; PER_PEAK : the ion mobility value is associated with each individual peak", required = false, converter = MobilityRepresentationMethodConverter.class)
  MobilityRepresentationMethod mobilityMethod = MobilityRepresentationMethod.NONE;

  @Parameter(names = {"-f","--file"}, description = "timstof file to convert", required = true)
  String filename;

}
