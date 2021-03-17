using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace NewServiceTag
{
    static class Program
    {
        /// <summary>
        /// Punto de entrada principal para la aplicación.
        /// </summary>
        static void Main()
        {
            #if DEBUG
                NewServiceTag debug = new NewServiceTag();
                debug.OnDebug();
            #else
                ServiceBase[] ServicesToRun;
                ServicesToRun = new ServiceBase[]
                {
                    new NewServiceTag()
                };
                ServiceBase.Run(ServicesToRun);
            #endif
        }
    }
}
