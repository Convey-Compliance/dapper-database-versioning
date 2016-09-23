using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DapperDatabaseVersioning.Utils
{
    internal class DBVersionDTO
    {
        public string ScriptName { get; set; }
        public DateTime DateExecuted { get; set; }
        public string ScriptMD5 { get; set; }
    }
}
