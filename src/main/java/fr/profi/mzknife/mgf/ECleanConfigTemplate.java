package fr.profi.mzknife.mgf;

import java.util.ArrayList;
import java.util.List;

public enum ECleanConfigTemplate {

    LABEL_FREE_CONFIG(),
    XLINK_CONFIG(),
    TMT_LABELLING_CONFIG();


    ECleanConfigTemplate(){
    }

    public List<String> stringifyParametersList(){
        return new ArrayList<>();
    }
}
