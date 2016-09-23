using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace DapperDatabaseVersioning.Utils
{
    internal class ScriptNameComparer : IComparer<string>
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
}
