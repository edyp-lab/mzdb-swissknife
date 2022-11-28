package fr.profi.mzknife;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;

import java.util.Arrays;
import java.util.stream.Collectors;

public class PCleanConfigConverter implements IStringConverter<CommandArguments.PCleanConfig> {

    @Override
    public CommandArguments.PCleanConfig convert(String value) {
        CommandArguments.PCleanConfig convertedValue = CommandArguments.PCleanConfig.getConfigFor(value);

        if (convertedValue == null) {
            String listValue = "";
            for (CommandArguments.PCleanConfig next : CommandArguments.PCleanConfig.values()) {
                listValue+= next.getCommandValue()+" ";
            }
            throw new ParameterException("Value " + value + "can not be converted to PClean Configuration. Available values are:" + listValue);
        }
        return convertedValue;
    }
}
