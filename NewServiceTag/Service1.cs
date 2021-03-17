using Oracle.ManagedDataAccess.Client;
using Service_Tags;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using System.Timers;

namespace NewServiceTag
{
    public partial class NewServiceTag : ServiceBase
    {
        private System.Timers.Timer timProcess = null;
        private int i = 0;
        private string path = @"C:\NewServiceFiles\";
        private string archivo = "WindowsServiceTagsPruebasNuevaVersion.txt";
        private bool SinRegistro = false;
        private static int Consecutivotxt = 0;

        public NewServiceTag()
        {
            InitializeComponent();
        }
        public void OnDebug()
        {
            MetodoInicial();
        }

        protected override void OnStart(string[] args)
        {
            timProcess = new System.Timers.Timer
            {
                Interval = 90000
            };
            timProcess.Elapsed += new System.Timers.ElapsedEventHandler(TimProcess_Elapsed);
            timProcess.Enabled = true;
            timProcess.Start();
        }

        private void TimProcess_Elapsed(object sender, ElapsedEventArgs e)
        {
            timProcess.Enabled = false;
            ExecuteProcess();
        }

        protected override void OnStop()
        {

        }

        private void StopService()
        {
            ServiceController sc = new ServiceController("NewServiceTag");
            try
            {
                if (sc != null && sc.Status == ServiceControllerStatus.Running)
                {
                    sc.Stop();
                }
                sc.WaitForStatus(ServiceControllerStatus.Stopped);
                sc.Close();
            }
            catch (Exception ex)
            {
                using (StreamWriter file = new StreamWriter(path + archivo, true))
                {
                    Consecutivotxt++;
                    file.WriteLine("erro al detener el servicio"); //se agrega información al documento
                    file.Dispose();
                    file.Close();
                }

            }
            StopService();
        }

        private void ExecuteProcess()
        {
            MetodoInicial();
            timProcess.Enabled = true;
        }

        public void MetodoInicial()
        {
            //Crea el Log si no Existe
            Buscar_Texto();
            var Bandera = Buscar_Bandera();
            //var Bandera = "hola";
            string Query;
            if (Bandera == null)

                Query = "SELECT CONTENU_ISO, VOIE, ID_GARE, TAB_ID_CLASSE, TO_CHAR(DATE_TRANSACTION, 'dd/mm/yyyy hh24:mi:ss')DATE_TRANSACTION, PRIX_TOTAL, EVENT_NUMBER, TAG_TRX_NB, INDICE_SUITE FROM  TRANSACTION Where  ID_PAIEMENT = '15' AND TO_CHAR(DATE_TRANSACTION, 'YYYY/MM/DD HH24:MI:SS' ) >= '2019/04/21 00:00:00'  AND TO_CHAR(DATE_TRANSACTION, 'YYYY/MM/DD HH24:MI:SS' ) < '2019/04/26 00:00:00' AND SUBSTR(TO_CHAR(CONTENU_ISO),0,3) = '501' and TAB_ID_CLASSE >=1 order by DATE_TRANSACTION ASC";
            else
            {
                Query = @"SELECT CONTENU_ISO, VOIE, ID_GARE, TAB_ID_CLASSE, TO_CHAR(DATE_TRANSACTION, 'dd/mm/yyyy hh24:mi:ss')DATE_TRANSACTION, PRIX_TOTAL, EVENT_NUMBER, TAG_TRX_NB, INDICE_SUITE
                        FROM  TRANSACTION
                        Where  ID_PAIEMENT = '15'
                        AND TO_CHAR(DATE_TRANSACTION, 'YYYY/MM/DD HH24:MI:SS' ) > '" + Bandera + "'  AND SUBSTR(TO_CHAR(CONTENU_ISO),0,3) = '501' AND TAB_ID_CLASSE >= 1";
            }

            var Cruces = Buscar_Cruces(Query);
            var crucesOrden = Cruces.OrderBy(item => item.Fecha);
            string SQL = "Data Source=.;Initial Catalog=GTDB; Integrated Security=False;User Id=Sa;Password=CAPUFE";
            //string SQL = "Data Source=.;Initial Catalog=GTDBPruebas; Integrated Security=False;User Id=Sa;Password=CAPUFE";
            SqlConnection ConexionSQL = new SqlConnection(SQL);
            try
            {
                using (SqlCommand cmd = new SqlCommand("", ConexionSQL))
                {
                    ConexionSQL.Open();
                    foreach (var item in crucesOrden)
                    {
                        Query = @"SELECT COUNT(*) FROM dbo.Historico 
	                        WHERE Id IN (SELECT Id FROM dbo.Historico  WHERE CONVERT(DATE, Fecha, 102) = '" + item.Fecha.ToString("yyyy-MM-dd") + "') " +
                                "AND (Fecha = '" + item.Fecha.ToString("dd/MM/yyyy HH:mm:ss") + "' AND Evento = '" + item.Evento + "' AND Tag = '" + item.NumTag + "'  AND Carril = '" + item.Carril + "' AND Clase = '" + Buscar_Clase(item.Clase) + "')";

                        cmd.CommandText = Query;
                        var Valida = Convert.ToInt32(cmd.ExecuteScalar());

                        if (Valida == 0)
                        {
                            Actualizar(new Historico
                            {
                                NumTag = item.NumTag,
                                Delegacion = item.Delegacion,
                                Plaza = item.Plaza,
                                Tramo = item.Tramo,
                                Carril = item.Carril,
                                Clase = item.Clase,
                                Fecha = Convert.ToDateTime(item.Fecha),
                                Evento = item.Evento,
                                Saldo = item.Saldo,
                                Operadora = item.Operadora,
                                TAG_TRX_NB = item.TAG_TRX_NB
                            });
                        }

                        using (StreamWriter file = new StreamWriter(path + archivo, true))
                        {
                            file.WriteLine("Proceso completo a las " + DateTime.Now.ToString());
                            file.Dispose();
                            file.Close();

                        }
                    }
                }
            }
            catch (Exception Ex)
            {
                using (StreamWriter file = new StreamWriter(path + archivo, true))
                {
                    Consecutivotxt++;
                    file.WriteLine("Prblema Limpiando la lista : " + Consecutivotxt.ToString() + " a las " + DateTime.Now.ToString() + " " + Ex.Message + " " + Ex.StackTrace + " " + Query); //se agrega información al documento
                    file.Dispose();
                    file.Close();
                }
                StopService();
            }
            finally
            {
                ConexionSQL.Close();
            }
        }

        public void Buscar_Texto()
        {
            try
            {
                if (!Directory.Exists(path))
                {
                    Directory.CreateDirectory(path);
                    if (!File.Exists(path + archivo))
                    {
                        File.CreateText(path + archivo);
                    }
                }
            }
            catch (Exception Ex)
            {
                using (StreamWriter file = new StreamWriter(path + archivo, true))
                {
                    Consecutivotxt++;
                    file.WriteLine("Se ejecuto el proceso ServicioWinTags: " + Consecutivotxt.ToString() + " a las " + DateTime.Now.ToString() + Ex.Message + " " + Ex.StackTrace); //se agrega información al documento
                    file.Dispose();
                    file.Close();
                }
                StopService();
            }
        }

        public Bandera Buscar_Bandera()
        {
            string SQL = "Data Source=.;Initial Catalog=GTDB; Integrated Security=False;User Id=Sa;Password=CAPUFE";
            //string SQL = "Data Source=.;Initial Catalog=GTDBPruebas; Integrated Security=False;User Id=Sa;Password=CAPUFE";
            SqlConnection ConexionSQL = new SqlConnection(SQL);
            using (SqlCommand SqlCommand = new SqlCommand("select top(1) convert(varchar,Fecha,27) as Fecha, Evento from Historico order by Fecha desc", ConexionSQL))
            {
                try
                {
                    ConexionSQL.Open();
                    using (var reader = SqlCommand.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            return new Bandera(Convert.ToDateTime(reader["Fecha"]), reader["Evento"].ToString());
                        }
                    }

                }
                catch (Exception Ex)
                {
                    using (StreamWriter file = new StreamWriter(path + archivo, true))
                    {
                        file.WriteLine("Error en el proceso ServicioWinProsis: " + Consecutivotxt.ToString() + " a las " + DateTime.Now.ToString() + " " + Ex.Message + " " + Ex.StackTrace + "" + "Bandera"); //se agrega información al documento
                        file.Dispose();
                        file.Close();
                    }
                    StopService();
                }
                finally
                {
                    ConexionSQL.Close();
                }

                return null;
            }
        }

        public List<Historico> Buscar_Cruces(string Query)
        {
            string Error = string.Empty;
            string SinError = string.Empty;
            try
            {
                string ORACLE = "User Id = GEADBA; Password = fgeuorjvne; Data Source = (DESCRIPTION = (ADDRESS_LIST = (ADDRESS = (PROTOCOL = TCP)(HOST= 10.1.10.111 )(PORT = 1521)))(CONNECT_DATA = (SERVER = DEDICATED)(SERVICE_NAME = GEAPROD)))";
                //string ORACLE = "User Id = GEADBA; Password = fgeuorjvne; Data Source = (DESCRIPTION = (ADDRESS_LIST = (ADDRESS = (PROTOCOL = TCP)(HOST= prosis.onthewifi.com )(PORT = 1521)))(CONNECT_DATA = (SERVER = DEDICATED)(SERVICE_NAME = GEAPROD)))";
                OracleConnection ConexionOracle = new OracleConnection(ORACLE);
                using (OracleCommand command = new OracleCommand(Query, ConexionOracle))

                {
                    try
                    {
                        ConexionOracle.Open();
                        command.ExecuteNonQuery();
                        DataTable dt = new DataTable("RECLASIFICADOS");
                        OracleDataAdapter myAdapter = new OracleDataAdapter(command);
                        myAdapter.Fill(dt);
                        /********************************************************/
                        foreach (DataRow indi in dt.Rows)
                        {
                            if (indi["CONTENU_ISO"].ToString().Replace(" ", "").Substring(0, 4) == "IMDM")
                            {
                                indi["CONTENU_ISO"] = indi["CONTENU_ISO"].ToString().Replace(" ", "").Substring(0, 12);
                            }
                            else if (indi["CONTENU_ISO"].ToString().Replace(" ", "").Substring(0, 4) == "OHLM")
                            {
                                indi["CONTENU_ISO"] = indi["CONTENU_ISO"].ToString().Replace(" ", "").Substring(0, 12);
                            }
                            else
                            {
                                indi["CONTENU_ISO"] = indi["CONTENU_ISO"].ToString().Replace(" ", "").Substring(0, 3) + indi["CONTENU_ISO"].ToString().Replace(" ", "").Substring(5, 8);
                                var prueba = indi["CONTENU_ISO"].ToString().Substring(6, 5);
                            }
                        }
                        DataTable dtDelegacion = new DataTable();
                        DataTable dtPlaza = new DataTable();
                        command.CommandText = "Select * From TYPE_RESEAU";
                        command.ExecuteNonQuery();
                        myAdapter.Fill(dtDelegacion);
                        command.CommandText = "Select * From TYPE_SITE";
                        command.ExecuteNonQuery();
                        myAdapter.Fill(dtPlaza);
                        string Delegacion = string.Empty;
                        string Plaza = string.Empty;
                        foreach (DataRow indi in dtDelegacion.Rows)
                        {
                            Delegacion = indi["NOM_RESEAU"].ToString();
                        }
                        foreach (DataRow indi in dtPlaza.Rows)
                        {
                            Plaza = indi["NOM_SITE"].ToString();
                        }
                        // METODO QUE AGREGA A LISTA DE CRUCES 
                        List<Historico> ListaHistorico = new List<Historico>();
                        foreach (DataRow item in dt.Rows)
                        {
                            Historico newRegistro = new Historico();
                            newRegistro.NumTag = item["CONTENU_ISO"].ToString();
                            newRegistro.Carril = item["VOIE"].ToString();
                            newRegistro.Delegacion = Delegacion;
                            newRegistro.Plaza = Plaza;
                            newRegistro.Tramo = item["ID_GARE"].ToString();
                            newRegistro.Clase = item["TAB_ID_CLASSE"].ToString();
                            DateTime date = DateTime.ParseExact(item["DATE_TRANSACTION"].ToString(), "dd/MM/yyyy HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);
                            newRegistro.Fecha = date;
                            newRegistro.Evento = item["EVENT_NUMBER"].ToString();
                            Error = item["PRIX_TOTAL"].ToString();
                            newRegistro.Saldo = double.Parse(double.Parse(item["PRIX_TOTAL"].ToString().Replace(".", ","), new NumberFormatInfo { NumberDecimalSeparator = "," }).ToString("F2"));
                            SinError = "Sin Error";
                            if (item["CONTENU_ISO"].ToString().Substring(0, 4) == "IMDM")
                                newRegistro.Operadora = "Otros";
                            else
                                newRegistro.Operadora = "SIVA";
                            // AGREGAMOS TAG_TRX_NB
                            newRegistro.TAG_TRX_NB = long.Parse(item["TAG_TRX_NB"].ToString());
                            ListaHistorico.Add(newRegistro);
                            Error = string.Empty;
                            SinError = string.Empty;
                        }
                        return ListaHistorico;
                    }
                    catch (Exception Ex)
                    {
                        using (StreamWriter file = new StreamWriter(path + archivo, true))
                        {
                            file.WriteLine("Error en el proceso ServicioWinProsis: " + Consecutivotxt.ToString() + " a las " + DateTime.Now.ToString() + " " + Ex.Message + " " + Ex.StackTrace + " " + "Buscar Cruces" + Error + SinError); //se agrega información al documento
                            file.Dispose();
                            file.Close();
                        }
                        StopService();
                    }
                    finally
                    {
                        ConexionOracle.Close();
                    }

                    return null;
                }
            }
            catch (Exception Ex)
            {
                using (StreamWriter file = new StreamWriter(path + archivo, true))
                {
                    file.WriteLine("Error en el proceso ServicioWinProsis: " + Consecutivotxt.ToString() + " a las " + DateTime.Now.ToString() + " " + Ex.Message + " " + Ex.StackTrace + " " + "Busqueda de Cruce en SQL"); //se agrega información al documento
                    file.Dispose();
                    file.Close();
                }
                StopService();
            }

            return null;
        }

        public TagCuenta Busca_TagCuenta(string Cruce, double saldo)
        {
            try
            {
                string Query = string.Empty;
                string SQL = "Data Source=.;Initial Catalog=GTDB; Integrated Security=False;User Id=Sa;Password=CAPUFE";
                //string SQL = "Data Source=.;Initial Catalog=GTDBPruebas; Integrated Security=False;User Id=Sa;Password=CAPUFE";
                SqlConnection ConexionSQL = new SqlConnection(SQL);
                Query = "Select top(1) CuentaId, NumTag, NumCuenta, StatusTag, StatusCuenta, TypeCuenta, SaldoCuenta, SaldoTag " +
                        "From Tags t Inner Join CuentasTelepeajes c on t.CuentaId = c.Id Where t.NumTag = '" + Cruce + "'";

                using (SqlCommand cmd = new SqlCommand(Query, ConexionSQL))
                {
                    try
                    {
                        ConexionSQL.Open();
                        using (var reader = cmd.ExecuteReader())
                        {
                            if (reader.Read())
                            {
                                if (reader["TypeCuenta"].ToString() == "Individual")
                                {
                                    return new TagCuenta(

                                        Convert.ToInt64(reader["CuentaId"].ToString()),
                                        Convert.ToString(reader["NumTag"]),
                                        Convert.ToString(reader["NumCuenta"]),
                                        Convert.ToBoolean(reader["StatusTag"]),
                                        Convert.ToBoolean(reader["StatusCuenta"]),
                                        Convert.ToString(reader["TypeCuenta"]),
                                        0,
                                        double.Parse((Convert.ToDouble(Convert.ToString(reader["SaldoTag"])) / 100.00).ToString("F2")),
                                        saldo
                                    );
                                }
                                else
                                {
                                    return new TagCuenta(

                                        Convert.ToInt64(reader["CuentaId"].ToString()),
                                        Convert.ToString(reader["NumTag"]),
                                        Convert.ToString(reader["NumCuenta"]),
                                        Convert.ToBoolean(reader["StatusTag"]),
                                        Convert.ToBoolean(reader["StatusCuenta"]),
                                        Convert.ToString(reader["TypeCuenta"]),
                                        double.Parse((Convert.ToDouble(Convert.ToString(reader["SaldoCuenta"])) / 100.00).ToString("F2")),
                                        double.Parse((Convert.ToDouble(Convert.ToString(reader["SaldoTag"])) / 100.00).ToString("F2")),
                                        saldo
                                    );
                                }
                            }
                            else
                            {
                                return null;
                            }
                        }
                    }
                    catch (Exception Ex)
                    {
                        using (StreamWriter file = new StreamWriter(path + archivo, true))
                        {
                            file.WriteLine("Error en el proceso ServicioWinProsis: " + Consecutivotxt.ToString() + " a las " + DateTime.Now.ToString() + " " + Ex.Message + " " + Ex.StackTrace + " " + "Busqueda de Cruce en SQL"); //se agrega información al documento
                            file.Dispose();
                            file.Close();
                        }
                        StopService();

                    }
                    finally
                    {
                        ConexionSQL.Close();
                    }
                }

                return null;
            }
            catch (Exception Ex)
            {
                using (StreamWriter file = new StreamWriter(path + archivo, true))
                {
                    file.WriteLine("Error en el proceso ServicioWinProsis: " + Consecutivotxt.ToString() + " a las " + DateTime.Now.ToString() + " " + Ex.Message + " " + Ex.StackTrace + " " + "Busqueda de Cruce en SQL"); //se agrega información al documento
                    file.Dispose();
                    file.Close();
                }
                StopService();

            }
            return null;
        }

        public void Actualizar(Historico newRow)
        {
            string SQL = "Data Source=.;Initial Catalog=GTDB; Integrated Security=False;User Id=Sa;Password=CAPUFE";
            //string SQL = "Data Source=.;Initial Catalog=GTDBPruebas; Integrated Security=False;User Id=Sa;Password=CAPUFE";
            SqlConnection ConexionSQL = new SqlConnection(SQL);
            string Query = string.Empty;
            string SaldoAnterior = string.Empty;
            string SaldoActualizado = string.Empty;
            string NumeroCuenta = string.Empty;
            string TipoCuenta = string.Empty;

            var TagCuenta2 = Busca_TagCuenta(newRow.NumTag, newRow.Saldo);
            if (TagCuenta2 == null)
            {
                SinRegistro = true;
                using (StreamWriter file = new StreamWriter(path + archivo, true))
                {
                    file.WriteLine("Error en el proceso ServicioWinProsis: " + Consecutivotxt.ToString() + " a las " + DateTime.Now.ToString() + " " + "Sin Coincidencia de cruce con SQL"); //se agrega información al documento
                    file.Dispose();
                    file.Close();
                }
                StopService();
            }

            if (!SinRegistro)
            {
                switch (Convert.ToString(TagCuenta2.TypeCuenta))
                {
                    case "Colectiva":
                        SaldoAnterior = Convert.ToString(TagCuenta2.SaldoCuenta);
                        var NuevoSaldoColectivos = TagCuenta2.SaldoCuenta - TagCuenta2.DescuentoCruce;
                        SaldoActualizado = Convert.ToString(NuevoSaldoColectivos);
                        //CambiosNewColumn
                        NumeroCuenta = TagCuenta2.NumCuenta;
                        TipoCuenta = TagCuenta2.TypeCuenta;

                        if (NuevoSaldoColectivos < 15.25)
                        {
                            using (SqlCommand cmd = new SqlCommand(Query, ConexionSQL))
                            {
                                try
                                {
                                    ConexionSQL.Open();
                                    cmd.CommandText = "Update CuentasTelepeajes Set SaldoCuenta = '" + Convert.ToString(Math.Round((NuevoSaldoColectivos * 100), 2)) + "' Where NumCuenta = '" + TagCuenta2.NumCuenta + "'";
                                    cmd.ExecuteNonQuery();

                                    cmd.CommandText = "Update Tags Set SaldoTag = '" + Convert.ToString(Math.Round((NuevoSaldoColectivos * 100), 2)) + "' Where CuentaId = '" + TagCuenta2.CuentaId + "'";
                                    cmd.ExecuteNonQuery();


                                    if (ValidarExcentos(newRow.NumTag))
                                    {

                                        cmd.CommandText = "Update CuentasTelepeajes Set StatusCuenta = '0' where NumCuenta = '" + TagCuenta2.NumCuenta + "'";
                                        cmd.ExecuteNonQuery();


                                        cmd.CommandText = "Update Tags Set StatusTag = '0' where CuentaId = '" + TagCuenta2.CuentaId + "'";
                                        cmd.ExecuteNonQuery();
                                    }

                                }
                                catch (Exception Ex)
                                {
                                    using (StreamWriter file = new StreamWriter(path + archivo, true))
                                    {
                                        file.WriteLine("Error en el proceso ServicioWinProsis: " + Consecutivotxt.ToString() + " a las " + DateTime.Now.ToString() + " " + Ex.Message + " " + Ex.StackTrace + " " + "Actualizacion de colectivo <"); //se agrega información al documento
                                        file.Dispose();
                                        file.Close();
                                    }
                                    StopService();

                                }
                                finally
                                {
                                    ConexionSQL.Close();

                                }
                            }
                        }
                        else
                        {
                            using (SqlCommand cmd = new SqlCommand(Query, ConexionSQL))
                            {
                                try
                                {
                                    ConexionSQL.Open();

                                    cmd.CommandText = "Update CuentasTelepeajes Set SaldoCuenta = '" + Convert.ToString(Math.Round((NuevoSaldoColectivos * 100), 2)) + "' Where NumCuenta = '" + TagCuenta2.NumCuenta + "'";
                                    cmd.ExecuteNonQuery();

                                    cmd.CommandText = "Update Tags Set SaldoTag = '" + Convert.ToString(Math.Round((NuevoSaldoColectivos * 100), 2)) + "' Where CuentaId = '" + TagCuenta2.CuentaId + "'";
                                    cmd.ExecuteNonQuery();



                                }
                                catch (Exception Ex)
                                {
                                    using (StreamWriter file = new StreamWriter(path + archivo, true))
                                    {
                                        file.WriteLine("Error en el proceso ServicioWinProsis: " + Consecutivotxt.ToString() + " a las " + DateTime.Now.ToString() + " " + Ex.Message + " " + Ex.StackTrace + " " + "Actualizacion de Colectivos"); //se agrega información al documento
                                        file.Dispose();
                                        file.Close();
                                    }
                                    StopService();
                                }
                                finally
                                {
                                    ConexionSQL.Close();

                                }
                            }
                        }
                        break;

                    case "Individual":
                        SaldoAnterior = Convert.ToString(TagCuenta2.SaldoTag);
                        var NuevoSaldoIndividuales = TagCuenta2.SaldoTag - TagCuenta2.DescuentoCruce;
                        SaldoActualizado = Convert.ToString(NuevoSaldoIndividuales);
                        //CambiosNewColumn
                        NumeroCuenta = TagCuenta2.NumCuenta;
                        TipoCuenta = TagCuenta2.TypeCuenta;

                        if (NuevoSaldoIndividuales < 15.25)
                        {
                            using (SqlCommand cmd = new SqlCommand(Query, ConexionSQL))
                            {
                                try
                                {
                                    ConexionSQL.Open();

                                    cmd.CommandText = "Update Tags Set SaldoTag = '" + Convert.ToString(Math.Round((NuevoSaldoIndividuales * 100), 2)) + "' Where CuentaId = '" + TagCuenta2.CuentaId + "'";
                                    cmd.ExecuteNonQuery();

                                    if (ValidarExcentos(newRow.NumTag))
                                    {
                                        cmd.CommandText = "Update Tags Set StatusTag = '0' where NumTag = '" + TagCuenta2.NumTag + "'";
                                        cmd.ExecuteNonQuery();
                                    }

                                }
                                catch (Exception Ex)
                                {
                                    using (StreamWriter file = new StreamWriter(path + archivo, true))
                                    {
                                        file.WriteLine("Error en el proceso ServicioWinProsis: " + Consecutivotxt.ToString() + " a las " + DateTime.Now.ToString() + " " + Ex.Message + " " + Ex.StackTrace + " " + "Actualiazcion Individual <"); //se agrega información al documento
                                        file.Dispose();
                                        file.Close();
                                    }
                                    StopService();
                                }
                                finally
                                {
                                    ConexionSQL.Close();

                                }
                            }
                        }
                        else
                        {
                            using (SqlCommand cmd = new SqlCommand(Query, ConexionSQL))
                            {
                                try
                                {
                                    ConexionSQL.Open();

                                    cmd.CommandText = "Update Tags Set SaldoTag = '" + Convert.ToString(Math.Round((NuevoSaldoIndividuales * 100), 2)) + "' Where CuentaId = '" + TagCuenta2.CuentaId + "'";
                                    cmd.ExecuteNonQuery();

                                }
                                catch (Exception Ex)
                                {
                                    using (StreamWriter file = new StreamWriter(path + archivo, true))
                                    {
                                        file.WriteLine("Error en el proceso ServicioWinProsis: " + Consecutivotxt.ToString() + " a las " + DateTime.Now.ToString() + " " + Ex.Message + " " + Ex.StackTrace + " " + "Actualizacion Individual"); //se agrega información al documento
                                        file.Dispose();
                                        file.Close();
                                    }
                                    StopService();

                                }
                                finally
                                {
                                    ConexionSQL.Close();

                                }
                            }
                        }
                        break;

                    default:
                        break;

                }

                ActualizarHistorico(newRow, SaldoAnterior, SaldoActualizado, NumeroCuenta, TipoCuenta);
            }
        }

        public void ActualizarHistorico(Historico newRow, string SaldoAnterior, string SaldoActualizado, string NumeroCuenta, string TipoCuenta)
        {
            try
            {
                DataTable table = CreaDt();
                DataRow row = table.NewRow();
                row["Tag"] = newRow.NumTag.ToString();
                row["Carril"] = newRow.Carril.ToString();
                row["Delegacion"] = newRow.Delegacion.ToString();
                row["Plaza"] = newRow.Plaza.ToString();
                row["Cuerpo"] = newRow.Tramo.ToString();
                DateTime date = DateTime.ParseExact(newRow.Fecha.ToString("dd/MM/yyyy HH:mm:ss"), "dd/MM/yyyy HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);
                row["Fecha"] = date;
                row["Clase"] = Buscar_Clase(newRow.Clase);
                row["Evento"] = newRow.Evento.ToString();
                row["Saldo"] = newRow.Saldo.ToString();
                if (newRow.Operadora.ToString().Substring(0, 4) == "IMDM")
                    row["Operador"] = "Otros";
                else
                    row["Operador"] = "SIVA";
                row["SaldoAnterior"] = SaldoAnterior.Replace(".", ",");
                row["SaldoActualizado"] = SaldoActualizado.Replace(".", ",");
                row["NumeroCuenta"] = NumeroCuenta;
                row["TipoCuenta"] = TipoCuenta;
                //row["TAG_TRX_NB"] = newRow.TAG_TRX_NB;
                table.Rows.Add(row);

                string SQL = "Data Source=.;Initial Catalog=GTDB; Integrated Security=False;User Id=Sa;Password=CAPUFE";
                //string SQL = "Data Source=.;Initial Catalog=GTDBPruebas; Integrated Security=False;User Id=Sa;Password=CAPUFE";
                SqlConnection ConexionSQL = new SqlConnection(SQL);

                using (SqlCommand SqlCommand = new SqlCommand("", ConexionSQL))
                {
                    try
                    {
                        ConexionSQL.Open();
                        using (SqlBulkCopy sqlBulk = new SqlBulkCopy(ConexionSQL))
                        {
                            sqlBulk.BulkCopyTimeout = 1000;
                            sqlBulk.DestinationTableName = "Historico";
                            sqlBulk.WriteToServer(table);
                            sqlBulk.Close();
                        }
                    }
                    catch (Exception Ex)
                    {
                        using (StreamWriter file = new StreamWriter(path + archivo, true))
                        {
                            file.WriteLine("Error en el proceso ServicioWinProsis: " + Consecutivotxt.ToString() + " a las " + DateTime.Now.ToString() + " " + Ex.Message + " " + Ex.StackTrace + " " + "Insertar en Historico"); //se agrega información al documento
                            file.Dispose();
                            file.Close();

                        }
                        StopService();
                    }
                    finally
                    {
                        ConexionSQL.Close();
                    }
                }
            }
            catch (Exception Ex)
            {
                using (StreamWriter file = new StreamWriter(path + archivo, true))
                {
                    Consecutivotxt++;
                    file.WriteLine("Error en el proceso ServicioWinTags: " + Consecutivotxt.ToString() + " a las " + DateTime.Now.ToString() + Ex.Message + Ex.StackTrace); //se agrega información al documento
                    file.Dispose();
                    file.Close();
                }
                StopService();

            }
        }

        public bool ValidarExcentos(string Tag)
        {
            Tag = Tag.Substring(7, 4);
            if (Convert.ToInt32(Tag) >= 0001 && Convert.ToInt32(Tag) <= 0200)
                return false;
            else
                return true;
        }

        public DataTable CreaDt()
        {
            DataTable table = new DataTable("Historico");
            DataColumn Columna1;
            Columna1 = new DataColumn();
            Columna1.ColumnName = "Id";
            Columna1.DataType = System.Type.GetType("System.Int32");
            table.Columns.Add(Columna1);
            DataColumn Columna2;
            Columna2 = new DataColumn();
            Columna2.ColumnName = "Tag";
            Columna2.DataType = System.Type.GetType("System.String");
            table.Columns.Add(Columna2);
            DataColumn Columna3;
            Columna3 = new DataColumn();
            Columna3.ColumnName = "Delegacion";
            Columna3.DataType = System.Type.GetType("System.String");
            table.Columns.Add(Columna3);
            DataColumn Columna4;
            Columna4 = new DataColumn();
            Columna4.ColumnName = "Plaza";
            Columna4.DataType = System.Type.GetType("System.String");
            table.Columns.Add(Columna4);
            DataColumn Columna5;
            Columna5 = new DataColumn();
            Columna5.ColumnName = "Cuerpo";
            Columna5.DataType = System.Type.GetType("System.String");
            table.Columns.Add(Columna5);
            DataColumn Columna6;
            Columna6 = new DataColumn();
            Columna6.ColumnName = "Carril";
            Columna6.DataType = System.Type.GetType("System.String");
            table.Columns.Add(Columna6);
            DataColumn Columna7;
            Columna7 = new DataColumn();
            Columna7.ColumnName = "Fecha";
            Columna7.DataType = System.Type.GetType("System.DateTime");
            table.Columns.Add(Columna7);
            DataColumn Columna8;
            Columna8 = new DataColumn();
            Columna8.ColumnName = "Clase";
            Columna8.DataType = System.Type.GetType("System.String");
            table.Columns.Add(Columna8);
            DataColumn Columna9;
            Columna9 = new DataColumn();
            Columna9.ColumnName = "Evento";
            Columna9.DataType = System.Type.GetType("System.String");
            table.Columns.Add(Columna9);
            DataColumn Columna10;
            Columna10 = new DataColumn();
            Columna10.ColumnName = "Saldo";
            Columna10.DataType = System.Type.GetType("System.Double");
            table.Columns.Add(Columna10);
            DataColumn Columna11;
            Columna11 = new DataColumn();
            Columna11.ColumnName = "Operador";
            Columna11.DataType = System.Type.GetType("System.String");
            table.Columns.Add(Columna11);
            DataColumn Columna12;
            Columna12 = new DataColumn();
            Columna12.ColumnName = "SaldoAnterior";
            Columna12.DataType = System.Type.GetType("System.String");
            table.Columns.Add(Columna12);
            DataColumn Columna13;
            Columna13 = new DataColumn();
            Columna13.ColumnName = "SaldoActualizado";
            Columna13.DataType = System.Type.GetType("System.String");
            table.Columns.Add(Columna13);
            DataColumn Columna14;
            Columna14 = new DataColumn();
            Columna14.ColumnName = "NumeroCuenta";
            Columna14.DataType = System.Type.GetType("System.String");
            table.Columns.Add(Columna14);
            DataColumn Columna15;
            Columna15 = new DataColumn();
            Columna15.ColumnName = "TipoCuenta";
            Columna15.DataType = System.Type.GetType("System.String");
            table.Columns.Add(Columna15);

            return table;
        }

        public string Buscar_Clase(string Clase)
        {
            string Nueva_Clase = string.Empty;
            if (Clase == "1")
            {
                Nueva_Clase = "T01A";
            }
            else if (Clase == "2")
            {
                Nueva_Clase = "T02C";
            }
            else if (Clase == "3")
            {
                Nueva_Clase = "T03C";
            }
            else if (Clase == "4")
            {
                Nueva_Clase = "T04C";
            }
            else if (Clase == "5")
            {
                Nueva_Clase = "T05C";
            }
            else if (Clase == "6")
            {
                Nueva_Clase = "T06C";
            }
            else if (Clase == "7")
            {
                Nueva_Clase = "T07C";
            }
            else if (Clase == "8")
            {
                Nueva_Clase = "T08C";
            }
            else if (Clase == "9")
            {
                Nueva_Clase = "T09C";
            }
            else if (Clase == "10")
            {
                Nueva_Clase = "TL01A";
            }
            else if (Clase == "11")
            {
                Nueva_Clase = "TL02A";
            }
            else if (Clase == "12")
            {
                Nueva_Clase = "T02B";
            }
            else if (Clase == "13")
            {
                Nueva_Clase = "T03B";
            }
            else if (Clase == "14")
            {
                Nueva_Clase = "T04B";
            }
            else if (Clase == "15")
            {
                Nueva_Clase = "T01M";
            }
            else if (Clase == "16")
            {
                Nueva_Clase = "TPnnC";
            }
            else if (Clase == "17")
            {
                Nueva_Clase = "TLnnA";
            }
            else if (Clase == "18")
            {
                Nueva_Clase = "T01T";
            }
            else if (Clase == "19")
            {
                Nueva_Clase = "T01P";
            }
            else
            {
                Nueva_Clase = "Ups!";
            }
            return Nueva_Clase;
        }

        private void eventLog1_EntryWritten(object sender, System.Diagnostics.EntryWrittenEventArgs e) { }

    }
}
