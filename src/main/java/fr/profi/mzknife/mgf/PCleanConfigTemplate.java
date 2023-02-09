package fr.profi.mzknife.mgf;

import java.util.ArrayList;
import java.util.List;

public enum PCleanConfigTemplate {

    LABEL_FREE_CONFIG(true, true, true, false, false, true, true, false, true),
    XLINK_CONFIG(true, true, true, false, false, true, true, false, true),
    TMT_LABELLING_CONFIG(true, true, true, false, false, true, true, false, true);

    final Boolean imonFilter;
    final Boolean repFilter;
    final Boolean labelFilter;
    final Boolean lowWinFilter;
    final Boolean highWinFilter;
    final Boolean isoReduction;
    final Boolean chargeDeconv;
    final Boolean ionsMerge;
    final Boolean largerThanPrecursor;

    PCleanConfigTemplate(Boolean imonFilter, Boolean repFilter, Boolean labelFilter, Boolean lowWinFilter, Boolean highWinFilter, Boolean isoReduction, Boolean chargeDeconv, Boolean ionsMerge,Boolean largerThanPrecursor){
        this.imonFilter = imonFilter;
        this.repFilter = repFilter;
        this.labelFilter = labelFilter;
        this.lowWinFilter = lowWinFilter;
        this.highWinFilter = highWinFilter;
        this.isoReduction = isoReduction;
        this.chargeDeconv = chargeDeconv;
        this.ionsMerge = ionsMerge;
        this.largerThanPrecursor = largerThanPrecursor;
    }

    public Boolean getImonFilter() {
        return imonFilter;
    }

    public Boolean getRepFilter() {
        return repFilter;
    }

    public Boolean getLabelFilter() {
        return labelFilter;
    }

    public Boolean getLowWinFilter() {
        return lowWinFilter;
    }

    public Boolean getHighWinFilter() {
        return highWinFilter;
    }

    public Boolean getIsoReduction() {
        return isoReduction;
    }

    public Boolean getChargeDeconv() {
        return chargeDeconv;
    }

    public Boolean getIonsMerge() {
        return ionsMerge;
    }

    public Boolean getLargerThanPrecursor() {
        return largerThanPrecursor;
    }

    public List<String> stringifyParametersList(){
        List<String> paramsAsStr  = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        sb.append("pClean.imonFilter=").append(imonFilter);
        paramsAsStr.add(sb.toString());
        sb = new StringBuilder();
        sb.append("pClean.repFilter=").append(repFilter);
        paramsAsStr.add(sb.toString());
        sb = new StringBuilder();
        sb.append("pClean.labelFilter=").append(labelFilter);
        paramsAsStr.add(sb.toString());
        sb = new StringBuilder();
        sb.append("pClean.lowWinFilter=").append(lowWinFilter);
        paramsAsStr.add(sb.toString());
        sb = new StringBuilder();
        sb.append("pClean.highWinFilter=").append(highWinFilter);
        paramsAsStr.add(sb.toString());
        sb = new StringBuilder();
        sb.append("pClean.isoReduction=").append(isoReduction);
        paramsAsStr.add(sb.toString());
        sb = new StringBuilder();
        sb.append("pClean.chargeDeconv=").append(chargeDeconv);
        paramsAsStr.add(sb.toString());
        sb = new StringBuilder();
        sb.append("pClean.ionsMerge=").append(ionsMerge);
        paramsAsStr.add(sb.toString());
        sb = new StringBuilder();
        sb.append("pClean.largerThanPrecursor=").append(largerThanPrecursor);
        paramsAsStr.add(sb.toString());
        return paramsAsStr;
    }
}
