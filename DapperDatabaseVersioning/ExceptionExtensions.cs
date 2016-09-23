using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DapperDatabaseVersioning
{
    public static class ExceptionExtensions
    {
        /// <summary>
		/// Obtains the "most recent" exception of type <typeparamref name="TException"/> within the scope of <paramref name="e"/> or any of its nested inner exceptions, if it exists.
		/// </summary>
		/// <typeparam name="TException"></typeparam>
		/// <param name="e"></param>
		/// <returns></returns>
		public static TException FindRootException<TException>(this Exception e)
            where TException : Exception
        {
            while (e != null)
            {
                var inner = e as TException;
                if (inner != null) return inner;
                e = e.InnerException;
            }
            return null;
        }
    }
}
