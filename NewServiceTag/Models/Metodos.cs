using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Service_Tags
{

    public partial class Historico
    {
        public int Id { get; set; }
        public string NumTag { get; set; }
        public string Delegacion { get; set; }
        public string Plaza { get; set; }
        public string Tramo { get; set; }
        public string Carril { get; set; }
        public DateTime Fecha { get; set; }
        public string Clase { get; set; }
        public string Evento { get; set; }
        //public string Saldo { get; set; }
        public double Saldo { get; set; }
        public string Operadora { get; set; }
        public long TAG_TRX_NB { get; set; }

    }

    public partial class TagCuenta
    {
        public long CuentaId { get; set; }
        public string NumTag { get; set; }
        public string NumCuenta { get; set; }
        public bool StatusTag { get; set; }
        public bool StatusCuenta { get; set; }
        public string TypeCuenta { get; set; }
        public double SaldoCuenta { get; set; }
        public double SaldoTag { get; set; }
        public double DescuentoCruce { get; set; }

    }

    public partial class Bandera
    {
        public DateTime Bandera_Nueva { get; set; }
        public string Evento { get; set; }

    }

}
