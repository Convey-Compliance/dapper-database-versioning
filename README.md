# dapper-database-versioning
Simple database versioning tool using Dapper. Based on the ShipCompliant implementation using NHibernate

# Gaps
No backout strategy

Logging is very very primative (as in, `Console.WriteLine`)

Many more I'm sure

# Usage

`/scriptDirectory`: the directory that houses all of the SQL scripts to be run

`/testMode`: if specified, will rollback the transaction with the scripts run instead of committing

## Connection string information

`/server`

`/database`

`/user`

`/password`


Based on the information above, the process will find all *.sql files in the specified directory and run the scripts against the connection string information specified.

After running the script a log entry is created in a table called `DBVersions` (this table is bootstrapped by this process). Additional runs of this process with the same folder check against this versioning system to avoid running scripts twice.
