package fr.profi.mzknife.mgf;

import java.util.ArrayList;
import java.util.List;

public enum PCleanConfigTemplate {

    LABEL_FREE_CONFIG(true, true, true, false, false, true, true, false, true),
    XLINK_CONFIG(true, true, true, false, false, true, true, false, true),
    TMT_LABELLING_CONFIG(true, true, true, false, false, true, true, false, true);

    Boolean imonFilter;
    Boolean repFilter;
    Boolean labelFilter;
    Boolean lowWinFilter;
    Boolean highWinFilter;
    Boolean isoReduction;
    Boolean chargeDeconv;
    Boolean ionsMerge;
    Boolean largerThanPrecursor;

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
        sb.append("imonFilter: ").append(imonFilter).append("; ");
        paramsAsStr.add(sb.toString());
        sb = new StringBuilder();
        sb.append("repFilter: ").append(repFilter).append("; ");
        paramsAsStr.add(sb.toString());
        sb = new StringBuilder();
        sb.append("labelFilter: ").append(labelFilter).append("; ");
        paramsAsStr.add(sb.toString());
        sb = new StringBuilder();
        sb.append("lowWinFilter: ").append(lowWinFilter).append("; ");
        paramsAsStr.add(sb.toString());
        sb = new StringBuilder();
        sb.append("highWinFilter: ").append(highWinFilter).append("; ");
        paramsAsStr.add(sb.toString());
        sb = new StringBuilder();
        sb.append("isoReduction: ").append(isoReduction).append("; ");
        paramsAsStr.add(sb.toString());
        sb = new StringBuilder();
        sb.append("chargeDeconv: ").append(chargeDeconv).append("; ");
        paramsAsStr.add(sb.toString());
        sb = new StringBuilder();
        sb.append("ionsMerge: ").append(ionsMerge).append("; ");
        paramsAsStr.add(sb.toString());
        sb = new StringBuilder();
        sb.append("largerThanPrecursor: ").append(largerThanPrecursor).append("; ");
        paramsAsStr.add(sb.toString());
        return paramsAsStr;
    }
}
