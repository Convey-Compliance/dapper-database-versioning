using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Transactions;
using Dapper;

namespace DapperDatabaseVersioning
{
    public class ScriptNameComparer : IComparer<string>
    {
        private static readonly Regex ScriptNameRegex =
            new Regex(
                "^(?:([a-z]+)-)?" + // optional JIRA project prefix; the hyphen separator is ignored
                "(\\d+)" + // case number; inherently parseable as an int because it's only digits
                "(?:[_-]?([a-z0-9]+))??" +
                // optional non-greedy case number suffix (e.g. 123a, 123a, 123b, etc).  May or may not have a hyphen or underscore separator
                "(?:_([a-z]+))?" +
                // optional "normal" suffix, e.g. _data, _schema, etc.  We are now less picky, so you can add whatever suffix you want.
                ".sql$" // forces everything prior to .sql be matched
                , RegexOptions.IgnoreCase | RegexOptions.Compiled);

        public static bool CanParse(string name)
        {
            return ScriptNameRegex.IsMatch(name);
        }

        public int Compare(string x, string y)
        {
            if (x == null)
                return y == null ? 0 : 1;
            if (y == null)
                return -1;

            var match = ScriptNameRegex.Match(x);
            var otherMatch = ScriptNameRegex.Match(y);

            if (!match.Success)
                return otherMatch.Success ? 1 : 0;
            if (!otherMatch.Success)
                return -1;

            var comparison = 0;

            comparison = String.Compare(match.Groups[1].Value, otherMatch.Groups[1].Value,
                StringComparison.OrdinalIgnoreCase);
            if (comparison != 0) return comparison;

            comparison = int.Parse(match.Groups[2].Value).CompareTo(int.Parse(otherMatch.Groups[2].Value));
            if (comparison != 0) return comparison;

            comparison = string.Compare(match.Groups[3].Value, otherMatch.Groups[3].Value,
                StringComparison.OrdinalIgnoreCase);
            if (comparison != 0) return comparison;

            if (string.Equals(match.Groups[4].Value, "schema", StringComparison.OrdinalIgnoreCase)) return -1;
            if (string.Equals(otherMatch.Groups[4].Value, "schema", StringComparison.OrdinalIgnoreCase)) return 1;

            comparison = string.Compare(match.Groups[4].Value, otherMatch.Groups[4].Value,
                StringComparison.OrdinalIgnoreCase);

            return comparison;
        }
    }

    public class DatabaseScript : IComparable<DatabaseScript>
    {
        private static readonly HashAlgorithm MD5 = HashAlgorithm.Create("MD5");
        private static readonly ScriptNameComparer Comparer = new ScriptNameComparer();

        public DatabaseScript(FileInfo file)
        {
            if (file == null) throw new ArgumentNullException("file");
            if (!file.Exists)
                throw new ArgumentException(string.Format("File {0} does not exist.", file.FullName), "file");

            _file = file;
        }

        public string MD5Hash { get; private set; }

        public string Name
        {
            get { return _file.Name; }
        }

        public string FullName
        {
            get { return _file.FullName; }
        }

        public bool CanProcess
        {
            get { return ScriptNameComparer.CanParse(_file.Name); }
        }

        public IEnumerable<string> StatementBatches
        {
            get { return _statementBatches ?? new List<string>(); }
        }

        public void PreProcess()
        {
            if (!CanProcess)
                throw new InvalidOperationException(
                    string.Format("DB script {0} cannot be processed because its name could not be parsed",
                        _file.FullName));
            if (MD5 == null) throw new InvalidOperationException("MD5 hash algorithm could not be created");

            using (var stream = _file.OpenRead())
            {
                var batches = new List<string>();
                var currentBatch = new StringBuilder();

                using (var reader = new StreamReader(stream, Encoding.UTF8))
                {
                    string line;
                    while ((line = reader.ReadLine()) != null)
                    {
                        // Trimmed is used for various comparisons, while the original line is executed in case we are in the middle of a multi-line string that is whitespace-significant
                        var trimmed = line.Trim();

                        if (trimmed.StartsWith("--"))
                            continue;

                        var tranKeywords = new[] { "begin tran", "commit tran" };
                        if (tranKeywords.Any(s => trimmed.StartsWith(s, StringComparison.OrdinalIgnoreCase)))
                            throw new InvalidOperationException(
                                string.Format("DB script {0} has transaction keywords.  Transactions are not allowed.",
                                    _file.FullName));

                        if (trimmed.StartsWith("set ansi_padding off", StringComparison.OrdinalIgnoreCase))
                            throw new InvalidOperationException(
                                string.Format("DB script {0} has SET ANSI_PADDING OFF.  Bad developer!", _file.FullName));

                        if (string.Equals(trimmed, "go", StringComparison.OrdinalIgnoreCase))
                        {
                            // advance to the next batch
                            if (currentBatch.Length > 0)
                                batches.Add(currentBatch.ToString());

                            currentBatch.Clear();
                            continue;
                        }

                        currentBatch.AppendLine(line);
                    }

                    if (currentBatch.Length > 0)
                        batches.Add(currentBatch.ToString());
                }

                _statementBatches = batches;
            }

            // We only hash significant changes:  i.e. things we're actually going to execute.  Artificially reintroduce "GO" commands between batches so that grouping of batches becomes significant.
            var hash = MD5.ComputeHash(Encoding.UTF8.GetBytes(string.Join("\ngo\n", _statementBatches)));
            MD5Hash = BitConverter.ToString(hash).Replace("-", "");
        }

        int IComparable<DatabaseScript>.CompareTo(DatabaseScript other)
        {
            return Comparer.Compare(_file.Name, other == null ? null : other._file.Name);
        }

        private readonly FileInfo _file;
        private ICollection<string> _statementBatches;
    }

    /// <summary>
    /// Summary description for DatabaseScriptsProcess.
    /// </summary>
    public class DapperDatabaseVersioning
    {
        private class DBVersionDTO
        {
            public string ScriptName { get; set; }
            public DateTime DateExecuted { get; set; }
            public string ScriptMD5 { get; set; }
            public int? BatchCount { get; set; }
        }

        private static SqlConnection connection;

        public static void Main(string[] args)
        {
            var arguments = new Arguments(args);
            connection =
                new SqlConnection(string.Format("data source={0};initial catalog={1};user id={2};password={3}",
                    arguments["server"], arguments["database"], arguments["user"], arguments["password"]));

            Process(arguments);
        }

        public static void Process(Arguments args)
        {
            const string dirArg = "SCRIPTBASEDIR";
            const string typeArg = "TYPE";
            const string folderArg = "FOLDER";

            if (args[dirArg] == null)
                throw new ArgumentException("Missing argument /SCRIPTBASEDIR");

            var dir = args[dirArg];
            if (dir.EndsWith("\\"))
            {
                dir = dir.Remove(dir.Length - 1, 1);
            }
            var names = dir.Split('\\');

            Console.WriteLine("DBScripts for root dir {0}", names[names.Length - 1]);
            var scriptBaseDirInfo = new DirectoryInfo(dir);
            if (!scriptBaseDirInfo.Exists)
                throw new DirectoryNotFoundException(scriptBaseDirInfo.Name);


            DirectoryInfo latestDirectory = null;


            //Find latest script directory
            DateTime latestCreateDate = DateTime.MinValue;

            foreach (DirectoryInfo dinfo in scriptBaseDirInfo.GetDirectories())
            {
                if (args[folderArg] == null)
                {
                    DateTime thisDirDate = GetDirectoryDate(dinfo);
                    if (thisDirDate > latestCreateDate || null == latestDirectory)
                    {
                        latestCreateDate = thisDirDate;
                        latestDirectory = dinfo;
                    }
                }
                else //use explicit folder name specified at command line
                {
                    string folder = args[folderArg].ToString();
                    if (folder.ToLower() == dinfo.Name.ToLower())
                    {
                        latestDirectory = dinfo;
                        break;
                    }
                }
            }

            if (latestDirectory == null)
                throw new DirectoryNotFoundException("Could not find a subdirectory under directory " +
                                                     scriptBaseDirInfo.Name);

            var candidateScriptsToProcess = new List<DatabaseScript>();

            foreach (var script in latestDirectory.GetFiles("*.sql").Select(fi => new DatabaseScript(fi)))
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

            var testOnly = (args[typeArg] != null && args[typeArg] == "test");


            if (testOnly)
                Console.WriteLine("TEST ONLY...Database change not persisted.");

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
                            var results = connection.Execute(batch);
                            if (results != null)
                                Console.WriteLine((results.ToString()));
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

                if (oneBigTransaction != null && testOnly) oneBigTransaction.Dispose();
                else oneBigTransaction.Complete();
            }

        }

        private static DateTime GetDirectoryDate(DirectoryInfo dinfo)
        {
            DateTime result = DateTime.MinValue;
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
                versions = connection.Query<DBVersionDTO>(
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
                        connection.Query<DBVersionDTO>(
                            "select ScriptName, DateExecuted, ScriptMD5, BatchCount from DBVersions").ToList();
                }
                else
                    throw;
            }

            return versions;
        }

        private static void AddDbVersionEntry(DatabaseScript script)
        {
            connection.Execute(
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

            // new table (temporary name)
            connection.Execute(
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
