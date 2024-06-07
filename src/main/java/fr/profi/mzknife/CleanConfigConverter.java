package fr.profi.mzknife;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;

public class CleanConfigConverter implements IStringConverter<CommandArguments.CleanConfig> {

    @Override
    public CommandArguments.CleanConfig convert(String value) {
        CommandArguments.CleanConfig convertedValue = CommandArguments.CleanConfig.getConfigFor(value);

        if (convertedValue == null) {
            StringBuilder listValue = new StringBuilder();
            for (CommandArguments.CleanConfig next : CommandArguments.CleanConfig.values()) {
                listValue.append(next.getConfigCommandValue()).append(" ");
            }
            throw new ParameterException("Value " + value + "can not be converted to PClean Configuration. Available values are:" + listValue);
        }
        return convertedValue;
    }
}
