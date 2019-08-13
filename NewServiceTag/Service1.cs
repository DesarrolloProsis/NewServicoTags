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
        private string path = @"C:\ExecutedActionWS\";
        private string archivo = "WindowsServiceTagsPruebasNuevaVersion.txt";
        private bool SinRegistro = false;
        private static int Consecutivotxt = 0;
        private int CrucesIniciales = 0;
        private int CrucesRegistrados = 0;
        private DateTime BanderaTxt;
        public NewServiceTag()
        {
            InitializeComponent();

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
            ServiceController sc = new ServiceController("ServiceTags");

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
                Console.WriteLine("Error al detener el servicio:");
                Console.WriteLine(ex.Message);
            }
            StopService();
        }

        private void ExecuteProcess()
        {

            MetodoInicial();
            timProcess.Enabled = true;
        }

        public List<TagCuenta> MetodoInicial()
        {
            CrucesIniciales = 0;
            CrucesRegistrados = 0;
            Buscar_Texto();
            var Bandera = Buscar_Bandera();
            string Query = string.Empty;

            if (Bandera == null)

                Query = "SELECT CONTENU_ISO, VOIE, ID_GARE, TAB_ID_CLASSE, TO_CHAR(DATE_TRANSACTION, 'dd/mm/yyyy hh24:mi:ss')DATE_TRANSACTION, PRIX_TOTAL, EVENT_NUMBER, TAG_TRX_NB, INDICE_SUITE FROM  TRANSACTION Where  ID_PAIEMENT = '15' AND TO_CHAR(DATE_TRANSACTION, 'YYYY/MM/DD HH24:MI:SS' ) >= '2019/04/21 00:00:00'  AND TO_CHAR(DATE_TRANSACTION, 'YYYY/MM/DD HH24:MI:SS' ) < '2019/04/26 00:00:00' AND SUBSTR(TO_CHAR(CONTENU_ISO),0,3) = '501' and TAB_ID_CLASSE >=1 order by DATE_TRANSACTION ASC";
            //Query = "SELECT CONTENU_ISO, VOIE, ID_GARE, TAB_ID_CLASSE, TO_CHAR(DATE_TRANSACTION, 'dd/mm/yyyy hh24:mi:ss')DATE_TRANSACTION, PRIX_TOTAL, EVENT_NUMBER FROM  TRANSACTION Where  ID_PAIEMENT = '15' AND TO_CHAR(DATE_TRANSACTION, 'YYYY/MM/DD HH24:MI:SS' ) > '2019/04/03 09:00:00'  AND ACD_CLASS >= 1  order by DATE_TRANSACTION ASC";

            else

                Query = @"SELECT CONTENU_ISO, VOIE, ID_GARE, TAB_ID_CLASSE, TO_CHAR(DATE_TRANSACTION, 'dd/mm/yyyy hh24:mi:ss')DATE_TRANSACTION, PRIX_TOTAL, EVENT_NUMBER, TAG_TRX_NB, INDICE_SUITE
                        FROM  TRANSACTION
                        Where  ID_PAIEMENT = '15'
                        AND TO_CHAR(DATE_TRANSACTION, 'YYYY/MM/DD HH24:MI:SS' ) > '" + Convert.ToString(Bandera[0].Bandera_Nueva.AddMinutes(-18).ToString("yyyy/MM/dd HH:mm:ss")) + "' AND SUBSTR(TO_CHAR(CONTENU_ISO),0,3) = '501' AND TAB_ID_CLASSE >= 1 order by DATE_TRANSACTION ASC";

            var Cruces = Buscar_Cruces(Query);


            //string SQL = "Data Source=.;Initial Catalog=GTDB; Integrated Security=False;User Id=SA;Password=CAPUFE";
            string SQL = "Data Source=.;Initial Catalog=GTDBPruebas; Integrated Security=False;User Id=Sa;Password=CAPUFE";


            SqlConnection ConexionSQL = new SqlConnection(SQL);
            List<Historico> CrucesListos = new List<Historico>();

            try
            {

                using (SqlCommand cmd = new SqlCommand("", ConexionSQL))
                {
                    ConexionSQL.Open();

                    foreach (var item in Cruces)
                    {
                        Query = @"SELECT COUNT(*) FROM dbo.Historico 
	                        WHERE Id IN (SELECT Id FROM dbo.Historico  WHERE CONVERT(DATE, Fecha, 102) = '" + item.Fecha.ToString("yyyy-MM-dd") + "') " +
                                "AND (Fecha = '" + item.Fecha.ToString("dd/MM/yyyy HH:mm:ss") + "' AND Evento = '" + item.Evento + "' AND Tag = '" + item.NumTag + "'  AND Carril = '" + item.Carril + "' AND Clase = '" + Buscar_Clase(item.Clase) + "')";

                        cmd.CommandText = Query;
                        var Valida = Convert.ToInt32(cmd.ExecuteScalar());

                        using (StreamWriter file = new StreamWriter(path + archivo, true))
                        {
                            file.WriteLine(DateTime.Now.ToString() + " " + item.NumTag + " " + item.Evento + " " + Valida + "/n" + Query); //se agrega información al documento
                            file.Dispose();
                            file.Close();
                        }

                        if (Valida == 0)
                        {
                            CrucesListos.Add(new Historico
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

            if (CrucesListos != null && CrucesListos.Count > 0)
            {
                if (Bandera == null)
                {
                    BanderaTxt = DateTime.Now;
                    CrucesIniciales = CrucesListos.Count();
                }
                else
                {
                    BanderaTxt = Bandera[0].Bandera_Nueva;
                    CrucesIniciales = CrucesListos.Count();
                }


                Actualizar(CrucesListos);
            }

            return null;
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

        public List<Bandera> Buscar_Bandera()
        {



            //string SQL = "Data Source=.;Initial Catalog=GTDB; Integrated Security=False;User Id=SA;Password=CAPUFE";
            string SQL = "Data Source=.;Initial Catalog=GTDBPruebas; Integrated Security=False;User Id=Sa;Password=CAPUFE";

            SqlConnection ConexionSQL = new SqlConnection(SQL);
            List<Bandera> Lista = new List<Bandera>();
            Bandera NewObject = new Bandera();

            using (SqlCommand SqlCommand = new SqlCommand("SELECT convert(datetime,Fecha)Fecha, Evento FROM Historico WHERE Fecha = (SELECT MAX(Fecha) FROM Historico) group by Fecha, Evento", ConexionSQL))
            {
                try
                {
                    ConexionSQL.Open();
                    SqlCommand.ExecuteNonQuery();
                    SqlDataAdapter sqlData = new SqlDataAdapter(SqlCommand);
                    DataTable table = new DataTable();
                    sqlData.Fill(table);

                    if (table.Rows.Count > 0)
                    {

                        foreach (DataRow item in table.Rows)
                        {
                            Lista.Add(new Bandera
                            {
                                Bandera_Nueva = Convert.ToDateTime(item["Fecha"].ToString()),
                                Evento = item["Evento"].ToString()
                            });
                        }
                    }
                    else
                    {
                        return null;
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


                return Lista;

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
            finally
            {

            }
            return null;
        }


        public List<TagCuenta> Busca_TagCuenta(string Cruce, double saldo)
        {

            List<TagCuenta> ListaTagCuenta = new List<TagCuenta>();

            try
            {

                string Query = string.Empty;

                //string SQL = "Data Source=.;Initial Catalog=GTDB; Integrated Security=False;User Id=SA;Password=CAPUFE";
                string SQL = "Data Source=.;Initial Catalog=GTDBPruebas; Integrated Security=False;User Id=Sa;Password=CAPUFE";

                SqlConnection ConexionSQL = new SqlConnection(SQL);

                Query = "Select CuentaId, NumTag, NumCuenta, StatusTag, StatusCuenta, TypeCuenta, SaldoCuenta, SaldoTag " +
                        "From Tags t Inner Join CuentasTelepeajes c on t.CuentaId = c.Id Where t.NumTag = '" + Cruce + "'";

                using (SqlCommand cmd = new SqlCommand(Query, ConexionSQL))
                {
                    try
                    {
                        ConexionSQL.Open();
                        cmd.ExecuteNonQuery();
                        DataTable dt = new DataTable();
                        SqlDataAdapter myAdapter = new SqlDataAdapter(cmd);
                        myAdapter.Fill(dt);


                        if (dt.Rows.Count != 0)
                        {
                            var pruebas = dt.Rows[0]["TypeCuenta"].ToString();
                            var CasandoBug = dt.Rows[0]["NumTag"].ToString();

                            //if (CasandoBug == "50100000877")
                            //{
                            //    string lisds = "AQUI";
                            //}

                            //else if (CasandoBug == "50100000663")
                            //{
                            //    string istas = "OAQUI";
                            //}

                            if (dt.Rows[0]["TypeCuenta"].ToString() == "Individual")
                            {

                                ListaTagCuenta.Add(new TagCuenta
                                {
                                    CuentaId = Convert.ToInt64(dt.Rows[0]["CuentaId"].ToString()),
                                    NumTag = Convert.ToString(dt.Rows[0]["NumTag"]),
                                    NumCuenta = Convert.ToString(dt.Rows[0]["NumCuenta"]),
                                    StatusTag = Convert.ToBoolean(dt.Rows[0]["StatusTag"]),
                                    StatusCuenta = Convert.ToBoolean(dt.Rows[0]["StatusCuenta"]),
                                    TypeCuenta = Convert.ToString(dt.Rows[0]["TypeCuenta"]),
                                    SaldoCuenta = 0,
                                    SaldoTag = double.Parse((Convert.ToDouble(Convert.ToString(dt.Rows[0]["SaldoTag"])) / 100.00).ToString("F2")),
                                    DescuentoCruce = saldo
                                    //DescuentoCruce = Convert.ToDouble(saldo)
                                    //DescuentoCruce = Convert.ToDouble("15.25")

                                });
                            }
                            else
                            {
                                ListaTagCuenta.Add(new TagCuenta
                                {
                                    CuentaId = Convert.ToInt64(dt.Rows[0]["CuentaId"].ToString()),
                                    NumTag = Convert.ToString(dt.Rows[0]["NumTag"]),
                                    NumCuenta = Convert.ToString(dt.Rows[0]["NumCuenta"]),
                                    StatusTag = Convert.ToBoolean(dt.Rows[0]["StatusTag"]),
                                    StatusCuenta = Convert.ToBoolean(dt.Rows[0]["StatusCuenta"]),
                                    TypeCuenta = Convert.ToString(dt.Rows[0]["TypeCuenta"]),
                                    SaldoCuenta = double.Parse((Convert.ToDouble(Convert.ToString(dt.Rows[0]["SaldoCuenta"])) / 100.00).ToString("F2")),
                                    SaldoTag = double.Parse((Convert.ToDouble(Convert.ToString(dt.Rows[0]["SaldoTag"])) / 100.00).ToString("F2")),
                                    DescuentoCruce = saldo
                                    //DescuentoCruce = Convert.ToDouble(saldo)
                                    //DescuentoCruce = Convert.ToDouble("15.25")

                                });
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

                return ListaTagCuenta;
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

            return ListaTagCuenta;
        }

        public void Actualizar(List<Historico> Historicos)
        {


            //string SQL = "Data Source=.;Initial Catalog=GTDB; Integrated Security=False;User Id=SA;Password=CAPUFE";
            string SQL = "Data Source=.;Initial Catalog=GTDBPruebas; Integrated Security=False;User Id=Sa;Password=CAPUFE";

            SqlConnection ConexionSQL = new SqlConnection(SQL);
            string Query = string.Empty;
            string SaldoAnterior = string.Empty;
            string SaldoActualizado = string.Empty;
            string NumeroCuenta = string.Empty;
            string TipoCuenta = string.Empty;
            foreach (var item in Historicos)
            {

                var TagCuenta2 = Busca_TagCuenta(item.NumTag, item.Saldo);

                if (TagCuenta2.Count == 0)
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
                else
                {
                    SinRegistro = false;
                }


                foreach (var item2 in TagCuenta2)
                {
                    switch (Convert.ToString(item2.TypeCuenta))
                    {
                        case "Colectiva":

                            SaldoAnterior = Convert.ToString(item2.SaldoCuenta);
                            var NuevoSaldoColectivos = item2.SaldoCuenta - item2.DescuentoCruce;
                            SaldoActualizado = Convert.ToString(NuevoSaldoColectivos);
                            //CambiosNewColumn
                            NumeroCuenta = item2.NumCuenta;
                            TipoCuenta = item2.TypeCuenta;

                            if (NuevoSaldoColectivos < 15.25)
                            {


                                using (SqlCommand cmd = new SqlCommand(Query, ConexionSQL))
                                {
                                    try
                                    {
                                        ConexionSQL.Open();
                                        cmd.CommandText = "Update CuentasTelepeajes Set SaldoCuenta = '" + Convert.ToString(Math.Round((NuevoSaldoColectivos * 100), 2)) + "' Where NumCuenta = '" + item2.NumCuenta + "'";
                                        cmd.ExecuteNonQuery();

                                        cmd.CommandText = "Update Tags Set SaldoTag = '" + Convert.ToString(Math.Round((NuevoSaldoColectivos * 100), 2)) + "' Where CuentaId = '" + item2.CuentaId + "'";
                                        cmd.ExecuteNonQuery();


                                        if (ValidarExcentos(item.NumTag))
                                        {

                                            cmd.CommandText = "Update CuentasTelepeajes Set StatusCuenta = '0' where NumCuenta = '" + item2.NumCuenta + "'";
                                            cmd.ExecuteNonQuery();


                                            cmd.CommandText = "Update Tags Set StatusTag = '0' where CuentaId = '" + item2.CuentaId + "'";
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

                                        cmd.CommandText = "Update CuentasTelepeajes Set SaldoCuenta = '" + Convert.ToString(Math.Round((NuevoSaldoColectivos * 100), 2)) + "' Where NumCuenta = '" + item2.NumCuenta + "'";
                                        cmd.ExecuteNonQuery();

                                        cmd.CommandText = "Update Tags Set SaldoTag = '" + Convert.ToString(Math.Round((NuevoSaldoColectivos * 100), 2)) + "' Where CuentaId = '" + item2.CuentaId + "'";
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

                            SaldoAnterior = Convert.ToString(item2.SaldoTag);
                            var NuevoSaldoIndividuales = item2.SaldoTag - item2.DescuentoCruce;
                            SaldoActualizado = Convert.ToString(NuevoSaldoIndividuales);
                            //CambiosNewColumn
                            NumeroCuenta = item2.NumCuenta;
                            TipoCuenta = item2.TypeCuenta;



                            if (NuevoSaldoIndividuales < 15.25)
                            {


                                using (SqlCommand cmd = new SqlCommand(Query, ConexionSQL))
                                {
                                    try
                                    {
                                        ConexionSQL.Open();

                                        cmd.CommandText = "Update Tags Set SaldoTag = '" + Convert.ToString(Math.Round((NuevoSaldoIndividuales * 100), 2)) + "' Where CuentaId = '" + item2.CuentaId + "'";
                                        cmd.ExecuteNonQuery();

                                        if (ValidarExcentos(item.NumTag))
                                        {
                                            cmd.CommandText = "Update Tags Set StatusTag = '0' where NumTag = '" + item2.NumTag + "'";
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

                                        cmd.CommandText = "Update Tags Set SaldoTag = '" + Convert.ToString(Math.Round((NuevoSaldoIndividuales * 100), 2)) + "' Where CuentaId = '" + item2.CuentaId + "'";
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


                }
                if (SinRegistro == false)
                    ActualizarHistorico(Historicos, item.NumTag, item.Evento, SaldoAnterior, SaldoActualizado, NumeroCuenta, TipoCuenta);
            }

            using (StreamWriter file = new StreamWriter(path + archivo, true))
            {
                Consecutivotxt++;
                file.WriteLine("Se inicio el proceso ServicioWinTags: " + Consecutivotxt.ToString() + " a las " + BanderaTxt.ToString("dd/MM/yyy  hh:mm:ss.fff") + " Al iniciar Registro " + CrucesIniciales + " Cruces " + "Termino a las: " + DateTime.Now.ToString("dd/MM/yyy  hh:mm:ss.fff") + " Ingreso " + CrucesRegistrados + " Cruces "); //se agrega información al documento
                file.Dispose();
                file.Close();
            }




        }

        public void ActualizarHistorico(List<Historico> Lista, string Tag, string Evento, string SaldoAnterior, string SaldoActualizado, string NumeroCuenta, string TipoCuenta)
        {
            try
            {

                var List = Lista.Where(x => x.NumTag == Tag).Where(x => x.Evento == Evento).ToList();

                DataTable table = CreaDt();
                foreach (var item in List)
                {
                    DataRow row = table.NewRow();

                    row["Tag"] = item.NumTag.ToString();
                    row["Carril"] = item.Carril.ToString();
                    row["Delegacion"] = item.Delegacion.ToString();
                    row["Plaza"] = item.Plaza.ToString();
                    row["Cuerpo"] = item.Tramo.ToString();
                    DateTime date = DateTime.ParseExact(item.Fecha.ToString("dd/MM/yyyy HH:mm:ss"), "dd/MM/yyyy HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);
                    row["Fecha"] = date;
                    row["Clase"] = Buscar_Clase(item.Clase);
                    row["Evento"] = item.Evento.ToString();
                    row["Saldo"] = item.Saldo.ToString();
                    if (item.Operadora.ToString().Substring(0, 4) == "IMDM")
                        row["Operador"] = "Otros";
                    else
                        row["Operador"] = "SIVA";
                    row["SaldoAnterior"] = SaldoAnterior.Replace(".", ",");
                    row["SaldoActualizado"] = SaldoActualizado.Replace(".", ",");
                    row["NumeroCuenta"] = NumeroCuenta;
                    row["TipoCuenta"] = TipoCuenta;
                    //row["TAG_TRX_NB"] = item.TAG_TRX_NB;
                    table.Rows.Add(row);

                }

                //string SQL = "Data Source=.;Initial Catalog=GTDB; Integrated Security=False;User Id=SA;Password=CAPUFE";
                string SQL = "Data Source=.;Initial Catalog=GTDBPruebas; Integrated Security=False;User Id=Sa;Password=CAPUFE";
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

                        CrucesRegistrados++;

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
    }
}
