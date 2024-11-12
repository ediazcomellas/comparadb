#!/usr/bin/env python3
#
#comparadb is free software; you can redistribute it and/or modify
#it under the terms of the GNU General Public License as published by
#the Free Software Foundation; either version 3 of the License, or
#(at your option) any later version.
#
#This program is distributed in the hope that it will be useful,
#but WITHOUT ANY WARRANTY; without even the implied warranty of
#MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#GNU General Public License for more details.
#
#You should have received a copy of the GNU General Public License along
#with this program; if not, write to the Free Software Foundation, Inc.,
#51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
#
#Please report bugs to ediaz@ultreia.es

import sys
import mysql.connector
import argparse
import json



parser = argparse.ArgumentParser(description="comparadb: program to compare two mysql servers")
parser.add_argument("-l", "--log", type=str, help="logfile to store output", default=None)
parser.add_argument("-c", "--config", type=str, help="config file in json format", default="comparadb.conf")
args = parser.parse_args()

global logfile, file_log, configfile


def exit_and_close(exitcode: int):
    global logfile, file_log
    try:
        if logfile != None and file_log != None:
            file_log.close()
    except Exception as e:
        print("Error in closing the logfile: ", e)
    finally:
        sys.stdout = sys.__stdout__
        sys.exit(exitcode)

if args.config != None:
    configfile = args.config
else:
    configfile = "comparadb.conf"

try:
    with open(configfile, 'r') as f:
        config = json.load(f)
except Exception as e:
    print(f"Error opening config file {configfile}: {e}")
    exit_and_close(3)


config1 = config.get('configorigin')
config2 = config.get('configdestination')

if config1 is None or config2 is None:
    print(f"ERROR: Configuration options not found in config file {configfile}")
    exit_and_close(3)

config_logfile = config.get('logfile')

if args.log:
    logfile = args.log
elif config_logfile is not None:
    logfile = config_logfile
else:
    logfile = None

if logfile is not None:
    try:
        file_log = open(logfile, 'w')
        sys.stdout = file_log
    except Exception as e:
        print(f"ERROR setting the output to {logfile}: {e}")
        sys.exit(2)



#Create users with something like
#CREATE USER 'compareuser'@'%' IDENTIFIED BY 'comparepassword1';
#GRANT SELECT, SHOW DATABASES, SHOW TABLES, SHOW VIEW ON *.* TO 'compareuser'@'%';
#FLUSH PRIVILEGES;

# server connection configuration.

print("Connecting with origin server: ", end="")
try:
    conn1 = mysql.connector.connect(**config1)
    print("ok")
except mysql.connector.Error as err:
    print("failed to connect")
    print(f"Exception {err}")
    exit_and_close(3)
print("Connecting to replica server: ", end="")
try:
    conn2 = mysql.connector.connect(**config2)
    print("ok")
except mysql.connector.Error as err:
    print("failed to connect")
    print(f"Exception {err}")
    exit_and_close(3)

cursor1 = conn1.cursor()
cursor2 = conn2.cursor()


def get_databases(cursor):
    cursor.execute("SHOW DATABASES")
    return {db[0] for db in cursor.fetchall() if
            db[0] not in ["information_schema", "mysql", "performance_schema", "sys"]}


def get_tables(cursor, database):
    cursor.execute(f"USE `{database}`")
    cursor.execute("""
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_type = 'BASE TABLE' AND table_schema = %s
                """, (database,))
    return {table[0] for table in cursor.fetchall()}


def get_table_structure(cursor, database, table):
    cursor.execute(f"USE `{database}`")
    cursor.execute(f"SHOW CREATE TABLE `{table}`")
    return cursor.fetchone()[1]


def get_table_row_count(cursor, database, table):
    cursor.execute(f"USE {database}")
    cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
    return cursor.fetchone()[0]


def compare_servers(cursor1, cursor2):
    global struct1, struct2, tables1, tables2, count1, count2
    databases1 = get_databases(cursor1)
    databases2 = get_databases(cursor2)

    iguales=True

    if databases1 != databases2:
        print("WARNING: There are different databases between the two servers.")
        print("Databases in origin not in replica: ", databases1 - databases2)
        print("Databases in replica not in origin: ", databases2 - databases1)
        print("\nWe will proceed checking only databases that exist in origin server\n")
        iguales=False
    else:
        print("OK: The database list is identical in both servers.")


    for db in databases1:
        print("==================================================================")
        print(f"Checking database {db}")
        print(f" - Comparing the list of tables in {db}: ", end=" " )

        try:
            tables1 = get_tables(cursor1, db)
        except Exception as e:
            print(f"ERROR: Error retrieving the list of tables in database {bd} in the origin server: {e}")
            continue
        try:
            tables2 = get_tables(cursor2, db)
        except Exception as e:
            print(f"ERROR: Error retrieving the list of tables in database {bd} in the replica server: {e}")
            continue

        if tables1 != tables2:
            print(f"WARNING: There are different tables between servers for database '{db}'.")
            print("Tables in origin server that are not in the replica server: ", tables1 - tables2)
            print("Tables in the replica server that are not in the origin: ", tables2 - tables1)
            #return False
            print("We will proceed to check tables existing in origin server only\n")
            iguales=False
        else: print("OK Same list of tables in both servers")

        for table in tables1:
            # Comparar estructura de la tabla
            print(f" -- Checking that the table structure matches for table {db}.{table}: ", end=" ")
            try:
                struct1 = get_table_structure(cursor1, db, table)
            except Exception as e:
                print(f"ERROR: Error retrieving table structure for {db}.{table} in origin server: {e}")
                continue
            try:
                struct2 = get_table_structure(cursor2, db, table)
            except Exception as e:
                print(f"ERROR: Error retrieving table structure for {db}.{table} in replica server: {e}")
                continue

            if struct1 != struct2:
                print(f"WARNING: Different table structures detected for  '{db}.{table}' ")
                print("Structure in orign server:", struct1)
                print("Structure in replica server:", struct2)
                print(" --- We continue with the next table --- \n")
                iguales=False
                continue
            else: print ("OK: Same structure in the tables of both servers")

            print(f" --- Checking the number of rows in table {db}.{table}: ",end=" ")
            
            # Comparar conteo de registros
            try:
                count1 = get_table_row_count(cursor1, db, table)
            except Exception as e:
                print ("ERROR: Error counting number of rows for table {db}.{table} in the origin server: {e}")
                continue
            try:
                count2 = get_table_row_count(cursor2, db, table)
            except Exception as e:
                print("ERROR: Error counting the number of rows for table {db}.{table} in the replica server: {e}")
                continue

            try:
                if count1 != count2:
                    print(f"WARNING: The number of rows differ for '{db}.{table}'")
                    print(f"Origin server has got {count1} rows; Replica server has got {count2} rows.")
                    print(" ------- Checking next table--- \n")
                    iguales=False
                    continue
                else: print("OK: Same number of rows in both servers")
            except Exception as e:
                print("ERROR: Error comparing the row count for table {db}.{table}: {e}")
                iguales=False

    return(iguales)

print("Starting database comparison program\n")
# Ejecutar la comparación
result = compare_servers(cursor1, cursor2)

try:
    # Cerrar conexiones
    cursor1.close()
    cursor2.close()
    conn1.close()
    conn2.close()
except Exception as e:
    print(f"Error closing connections to the databases· {e}")


if result == True:
    print("")
    print("EVERYTHING CORRECT. Congratulations.")
    exit_and_close(0)
else:
    print("There are differences between the databases. Please check the log")
    exit_and_close(1)

