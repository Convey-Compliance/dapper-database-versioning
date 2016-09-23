using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace DapperDatabaseVersioning.Utils
{
    internal class DatabaseScript : IComparable<DatabaseScript>
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

}
