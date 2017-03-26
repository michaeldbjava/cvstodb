# csvtodb
This is the very beginning of deployment of a little helper application which has only one task. 
This task consists of the import of a CSV file to a table of relational database like MySQL, PostgreSQL or MS-SQL-Server. 

Future Request could be:
+ Create table if no aim table is defined. This should be an optional feature. Either you give Information of table and field mapping from csv to table or you give a Information that a table should be created. The XSD file must have some additional rules for it. The classes ConfigurationCsvToDB, CheckCsvToDBConfigurationInformation and ImporCSVToDb must be adapted.
+ Send confirmation if import is finished
+ It should be possible to configure output to standard output or an output file

Many thanks to the Apache Commons CSV project. Its a great library. This liddle project could not be possible in that easy war. 
