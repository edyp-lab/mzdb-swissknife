# mzdb-swissknife



## Getting started 

A set of tools for mzdb and mgf files. Use the `run.bat` batch file to run the available commands. From Windows command prompt (cmd) move to the mzdb-swissknife folder `cd mzdb-swissknife-1.1.2-SNAPSHOT`. 

To display the list of commands (and their parameters) type:\
* `run.bat mzdb --help` for commands applying to a mzdb files.
* `run.bat mgf --help` for commands applying to an MGF files.
* `run.bat maxquant --help` for commands applying to maxquant temporary/intermediate files.



## mzdb commands examples

To create an MGF file from a Label Free acquisition run named `HF2_020258` using the `mgf_boost` method but without pClean process on MS2 fragments use the following command: 

```
run.bat mzdb create_mgf -mztol 10 -ptitle -precmz mgf_boost -mzdb HF2_020258.mzdb -o HF2_020258_mzdb_3.6_v2.mgf
```

or with pClean enabled : 

```
run.bat mzdb create_mgf -mztol 10 -ptitle -precmz mgf_boost -pClean -mzdb HF2_020258.mzdb -o HF2_020258_mzdb_3.6_pclean_v2.mgf
```



To create an MGF file from a TMT 6 plex experiment using pClean: 

```
run.bat mzdb create_mgf -mztol 10 -ptitle -precmz mgf_boost -pClean -pLabelMethod TMT6plex -mzdb HF2_020259.mzdb -o HF2_020256_mzdb_3.6_pcleanTMT_v2.mgf
```



### MGF commands examples

to run pClean with the default parameters on the `HF2_020258_mzdb_3.6_v2.mgf` file : 

```
run.bat mgf pclean -mgf HF2_020258_mzdb_3.6_v2.mgf -o HF2_020258_mgf3.6v2_pcleanv2.mgf
```

The complete list of parameters and their default values are available by typing `run.bat mgf --help`

### MaxQuant command examples

To create an MGF file from the .apl files created by Maxquant, type: 

```
run.bat maxquant create_mgf -i1 Xpl1_002790.HCD.FTMS.sil0.apl -i2 Xpl1_002790.HCD.FTMS.peak.apl -o Xpl1_002790_MQ.mgf
```

