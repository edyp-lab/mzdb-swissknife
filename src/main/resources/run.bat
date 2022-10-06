@echo off

FOR /f "tokens=1,* delims= " %%a in ("%*") do SET ALL_BUT_FIRST=%%b

IF "%1" == "mzdb" (

  .\jdk\bin\java -Xmx2G -Duser.country=US  -Duser.language=en -cp "lib/*;mzdb-swissknife-${pom.version}.jar" -Dlogback.configurationFile=config/logback.xml fr.profi.mzknife.MzDbProcessing  %ALL_BUT_FIRST%

) ELSE IF "%1" == "mgf" (

  .\jdk\bin\java -Xmx2G -Duser.country=US  -Duser.language=en -cp "lib/*;mzdb-swissknife-${pom.version}.jar" -Dlogback.configurationFile=config/logback.xml fr.profi.mzknife.MGFProcessing  %ALL_BUT_FIRST%

) ELSE IF "%1" == "maxquant" (

  .\jdk\bin\java -Xmx2G -Duser.country=US  -Duser.language=en -cp "lib/*;mzdb-swissknife-${pom.version}.jar" -Dlogback.configurationFile=config/logback.xml fr.profi.mzknife.MaxQuantProcessing  %ALL_BUT_FIRST%

) ELSE (

  echo.
  echo %1 : unknow command, please use "mzdb", "mgf" or "maxquant" as command name followed by --help for displaying the command help.
  echo.

)