# dapper-database-versioning
Simple database versioning tool using Dapper. Based on the ShipCompliant implementation using NHibernate

# Gaps
No backout strategy
Many more I'm sure

# Usage

`/scriptDirectory`: the directory that houses all of the SQL scripts to be run

`/testMode`: if specified, will rollback the transaction with the scripts run instead of committing

## Connection string information

`/server`

`/database`

`/user`

`/password`
