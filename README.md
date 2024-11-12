# comparadb

Compare two database servers to find differences

This program is intended to help you diagnose differences between a master mysql server and its replica. 
The script will check all the databases, check tables, table definitions and the number of records in both servers. 
In case there are any differences, it will show them.

## Getting Started

Make sure you configure the config file with your connection parameters. You can optionally set the logfile
in case you don't want to see the output in stdout. 

### Prerequisites

This has been tested with python 3.11. You will need the mysql.connector. 
You can install the requirements creating a virtual environment with this commands

```
$ virtualenv <env_name>
$ source <env_name>/bin/activate
(<env_name>)$ pip install -r requirements.txt
```

### Running the comparison
Activate the virtual environment, edit the config file and run the program. 
You can specify a couple of parameters:
- -l LOGFILE or --log LOGFILE : store all the progress information and differences in the file named LOGFILE
- -c CONFIGFILE or --config CONFIGFILE: read the connection details from the JSON file named CONFIGFILE

If you don't specify a config file, it will try to load `comparadb.conf`


### Risks

I've created this script to detect obvious differences between the databases in two servers. 
This means that it will detect incoherences in:

- number and name of databases
- tables in each database
- table definition and auto_increments
- number of rows in each table

Comparing the content of each table requires much more programming effort and execution time, so 
it is not in the scope of this simple task. 

We use this script to check problems with partially replicated tables, in situations where doing
the standard replication process (that maintains table coherence) does not work. When we detect 
differences, we try to solve them manually. 

Please make extra checks on the coherence of your replication. Don't trust this script without
knowing the implications. 


