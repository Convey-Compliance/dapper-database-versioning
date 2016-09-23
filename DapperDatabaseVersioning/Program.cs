using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Transactions;
using Dapper;
using DapperDatabaseVersioning.Utils;

namespace DapperDatabaseVersioning
{
    public class DapperDatabaseVersioning
    {
        private static SqlConnection _connection;

        public static void Main(string[] args)
        {
            var arguments = new Arguments(args);
            _connection =
                new SqlConnection(
                    $"data source={arguments["server"]};initial catalog={arguments["database"]};user id={arguments["user"]};password={arguments["password"]}");

            Process(arguments);
        }

        private static void Process(Arguments args)
        {
            const string scriptDirectoryArg = "scriptDirectory";
            const string testMode = "testMode";

            if (args[scriptDirectoryArg] == null)
                throw new ArgumentException("Missing argument /scriptDirectory");

            var dir = args[scriptDirectoryArg];
            if (dir.EndsWith("\\"))
            {
                dir = dir.Remove(dir.Length - 1, 1);
            }

            var names = dir.Split('\\');
            Console.WriteLine("Running script in root directory {0}", names[names.Length - 1]);

            var scriptBaseDirInfo = new DirectoryInfo(dir);
            if (!scriptBaseDirInfo.Exists)
                throw new DirectoryNotFoundException(scriptBaseDirInfo.Name);
            

            var candidateScriptsToProcess = new List<DatabaseScript>();
            foreach (var script in scriptBaseDirInfo.GetFiles("*.sql").Select(fi => new DatabaseScript(fi)))
            {
                if (script.CanProcess)
                    candidateScriptsToProcess.Add(script);
                else
                    Console.WriteLine("Ignored script {0} because its name was unparseable.",
                        script.FullName);
            }

            candidateScriptsToProcess.Sort();

            var scriptsAlreadyRun = GetScriptsAlreadyRun();
            var joinedScripts =
                candidateScriptsToProcess.GroupJoin(scriptsAlreadyRun, ds => ds.Name, dbv => dbv.ScriptName,
                    (script, dbVersions) => new { script, dbVersion = dbVersions.FirstOrDefault() },
                    StringComparer.OrdinalIgnoreCase)
                    .ToList();

            var alreadyRun = joinedScripts.Where(x => x.dbVersion != null);
            var scriptsToRun = joinedScripts.Where(x => x.dbVersion == null).Select(x => x.script).ToList();

            foreach (var x in alreadyRun)
            {
                Console.WriteLine("Skipping {0}; already run.", x.script.Name);

                // Sanity check already run scripts -- warn if checksums don't match
                if (!string.IsNullOrEmpty(x.dbVersion.ScriptMD5))
                {
                    try
                    {
                        x.script.PreProcess();
                        if (x.script.MD5Hash != x.dbVersion.ScriptMD5)
                            Console.WriteLine(
                                "Checksum mismatch:  script {0} was modified after it was first run on {1}.",
                                x.script.Name, x.dbVersion.DateExecuted);
                    }
                    catch (Exception)
                    {
                        // only a sanity check, so swallow parsing errors, etc.
                    }
                }
            }
            
            // Now ensure we can process all scripts.
            foreach (var script in scriptsToRun)
                script.PreProcess();

            var testOnly = (args[testMode] != null);
            if (testOnly)
                Console.WriteLine("TEST MODE. Scripts will not be committed.");

            using (var oneBigTransaction = new TransactionScope())
            {
                foreach (var script in scriptsToRun)
                {
                    // run script
                    // check entity load errors
                    // update DBVersions
                    foreach (var batch in script.StatementBatches)
                    {
                        try
                        {
                            var results = _connection.Execute(batch);
                            Console.WriteLine(results.ToString());
                        }
                        catch (Exception ex)
                        {
                            var sqlException = ex.FindRootException<SqlException>();

                            if (sqlException != null)
                            {
                                var line = batch.Split('\n').ElementAtOrDefault(sqlException.LineNumber - 1);
                                Console.WriteLine(
                                    "Error running script {0}:\n===================================\nSource Line: {1}\n\nSQL Error: {2}\n===================================\n",
                                    script.Name, line, sqlException.Message);
                            }
                            else
                            {
                                Console.Write("Error running script {0}: {1}", script.Name, ex.StackTrace);
                            }

                            oneBigTransaction.Dispose();

                            // so we blow up the transaction and kill the process
                            throw;
                        }
                    }

                    AddDbVersionEntry(script);
                }

                if (testOnly) oneBigTransaction.Dispose();
                else oneBigTransaction.Complete();
            }

        }

        private static DateTime GetDirectoryDate(DirectoryInfo dinfo)
        {
            var result = DateTime.MinValue;
            string dateString = dinfo.Name;
            try
            {
                int year = Int32.Parse(dateString.Substring(0, 4));
                int month = Int32.Parse(dateString.Substring(4, 2));
                int day = Int32.Parse(dateString.Substring(6, 2));
                result = new DateTime(year, month, day);
            }
            catch
            {
                //if unable to parse return minimum date.
            }

            return result;
        }

        private static IList<DBVersionDTO> GetScriptsAlreadyRun()
        {
            IList<DBVersionDTO> versions;

            try
            {
                versions = _connection.Query<DBVersionDTO>(
                            "select ScriptName, DateExecuted, ScriptMD5, BatchCount from DBVersions").ToList();
            }
            catch (Exception ex)
            {
                // Update DBVersions automatically if we're dealing with a down-level schema
                var sqlException = ex.FindRootException<SqlException>();
                if (sqlException != null && sqlException.Message.StartsWith("Invalid object name"))
                {
                    CreateDbVersions();
                    versions =
                        _connection.Query<DBVersionDTO>(
                            "select ScriptName, DateExecuted, ScriptMD5, BatchCount from DBVersions").ToList();
                }
                else
                    throw;
            }

            return versions;
        }

        private static void AddDbVersionEntry(DatabaseScript script)
        {
            _connection.Execute(
                "insert into DBVersions (ScriptName, DateExecuted, ScriptMD5, BatchCount) values(@scriptName, @dateExecuted, @scriptMD5, @batchCount)",

                new
                {
                    scriptName = script.Name,
                    dateExecuted = DateTime.Now,
                    scriptMD5 = script.MD5Hash,
                    batchCount = script.StatementBatches.Count()
                }
                );
        }

        private static void CreateDbVersions()
        {
            Console.WriteLine("Creating DbVersions schema.");
            
            _connection.Execute(
                @"create table DBVersions
(
	ScriptName varchar(255) not null constraint PK_DBVersions primary key clustered,
	DateExecuted datetime not null,
	ScriptMD5 varchar(32) null,
	BatchCount int null
)");

        }
    }


}
