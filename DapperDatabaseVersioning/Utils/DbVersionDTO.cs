using System;

namespace DapperDatabaseVersioning.Utils
{
    internal class DBVersionDTO
    {
        public string ScriptName { get; set; }
        public DateTime DateExecuted { get; set; }
        public string ScriptMD5 { get; set; }
    }
}
