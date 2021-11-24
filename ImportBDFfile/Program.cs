using System;
using DbfDataReader;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.Data.OleDb;
using System.Linq;
using System.IO;
using System.Collections.Generic;
using DbfReader;
using System.Collections;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;


namespace ImportBDFfile
{
    class Program
    {
       static string sql, connectionString, dbfPath;
       static string TablaDestino,NombreArchivo,sucursal;
        private static ReaderWriterLockSlim _readWriteLock = new ReaderWriterLockSlim();
        static DataTable TablasHabilitadas = new DataTable();
        
        static void Main(string[] args)
        {
           IConfiguration config; config = new ConfigurationBuilder().AddJsonFile("appSettings.json").Build();
            dbfPath =config.GetSection("CarpetaLog").Value.Replace("/", @"\"); 
            string conn = "Data Source=farmatrixsql01.database.windows.net; Initial Catalog=farmatrixdw; User ID=kpena;Password=fQPWEjApzLZd3cnR;Connection Timeout=0;Connection Lifetime=0;Min Pool Size=0;Max Pool Size=1000;Pooling=true ; Encrypt=True;";
            //string conn = "Data Source=MSI-1387; Initial Catalog=FARMATRIX; Connection Timeout=0;Connection Lifetime=0;Min Pool Size=0;Max Pool Size=1000;Pooling=true ; Integrated Security=true;";
             //dbfPath = @"C:\Conexion\";
            var Directorio = "";
            var TablaExcepcion = "";
            var File = "";
            int Intentos = 0;
            //to iterate over the rows:
            var skipDeleted = true;
            var tareas = new List<Task>();

            var options = new DbfDataReaderOptions
            {
                SkipDeletedRecords = true
                // Encoding = EncodingProvider.GetEncoding(1252);
            };
            
            
            //string connectionString = @"Provider=VFPOLEDB.1;Data Source=C:\Conexion\";
            string dbfToConvert = dbfPath;

            // CargaFull(dbfToConvert, "inv_detmovimientos", conn);
            //dbfToConvert.Replace(".dbf", ".csv", RegexOptions.IgnoreCase)


            try
            {
                using (SqlConnection cn = new SqlConnection(conn))
                {

                    using (SqlCommand cmd = new SqlCommand("select * from [dbo].Tablas   where habilitado = '1'", cn)) //stg.TablaConfiguracion   -- [dbo].Tablas
                    {
                        cn.Open();
                        SqlDataAdapter ad = new SqlDataAdapter(cmd);

                        //DataTable dt = new DataTable();

                        ad.Fill(TablasHabilitadas);
                        int numero = 0;
                        if (TablasHabilitadas.Rows.Count > 0)
                        {
                            var Tablafull = TablasHabilitadas.AsEnumerable()
                                             .Where(x => x["TipoCarga"].ToString() == "F")
                                             .Select(x => new { tablausar = x["TablaDestino"].ToString() }).Distinct(); 

                            foreach (var tablaf in Tablafull)
                            {
                                TruncateTableFullCarga(tablaf.tablausar, conn);
                            }

                        }


                        while (numero < TablasHabilitadas.Rows.Count)
                        {                           
                            var rows = TablasHabilitadas.AsEnumerable()
                                        .Skip(numero).Take(6);
                            //DataTable dte = rows.CopyToDataTable();
                            DataRow[] dtw = rows.ToArray();

                            if (dtw.Count() > 0)
                            {
                                string sql1 = dtw[0]["SelectTable"].ToString();
                                string TablaDestino = dtw[0]["TablaDestino"].ToString();
                                string NombreArchivo = dtw[0]["NombreTabla"].ToString();
                                TablaExcepcion = dtw[0]["NombreTabla"].ToString();
                                 Directorio = dtw[0]["Directorio"].ToString();
                                string sucursal = dtw[0]["sucursal"].ToString();
                                sql1 = sql1 + string.Format(" ,'{0}' as sucursal", sucursal);
                                string columnaCondicion = dtw[0]["ColumnaCondicion"].ToString();
                                string ValorCondicion = dtw[0]["ValorCondicion"].ToString();
                                string dbfToConvert1 = string.Concat(Directorio, NombreArchivo);
                                string connectionString1 = string.Format(@"Provider=VFPOLEDB.1;Data Source= {0} ", Directorio);
                                string TipoCondicion = dtw[0]["TipoCondicion"].ToString();
                                string TipoCarga = dtw[0]["TipoCarga"].ToString();
                                string condicion="";
                                switch (TipoCondicion)
                                {
                                    case "F":
                                        //cuando el campo es date
                                        condicion = string.Format(" WHERE !Deleted() and  {0} > Ctod('{1}') ", columnaCondicion, ValorCondicion);
                                        break;
                                    case "E":
                                        //cuando el campo es dateTime
                                        condicion = string.Format(" WHERE !Deleted() and  {0} > {1} ", columnaCondicion, ValorCondicion);
                                        break;
                                    default:
                                        condicion = "";
                                        break;
                                }
                                

                                tareas.Add(Task.Factory.StartNew(() => ConvertDbf(connectionString1, dbfToConvert1, sql1, conn, TablaDestino, condicion,sucursal, columnaCondicion,TipoCarga)));
                               // tarea1 = Task.Factory.StartNew(() => ConvertDbf(connectionString1, dbfToConvert1, sql1, conn, TablaDestino,condicion));
                                //Thread hilo = new Thread(delegate () { ConvertDbf(connectionString1, dbfToConvert1, sql1, conn, TablaDestino); });
                                //hilo.Start();
                            }
                            if (dtw.Count() > 1)
                            {
                                string sql2 = dtw[1]["SelectTable"].ToString();
                                string TablaDestino = dtw[1]["TablaDestino"].ToString();
                                string NombreArchivo = dtw[1]["NombreTabla"].ToString();
                                TablaExcepcion = dtw[1]["NombreTabla"].ToString();
                                 Directorio = dtw[1]["Directorio"].ToString();
                                string sucursal = dtw[1]["sucursal"].ToString();
                                sql2 = sql2 + string.Format(" ,'{0}' as sucursal", sucursal);
                                string columnaCondicion = dtw[1]["ColumnaCondicion"].ToString();
                                string ValorCondicion = dtw[1]["ValorCondicion"].ToString();
                                string dbfToConvert2 = string.Concat(Directorio, NombreArchivo);
                                string connectionString2 = string.Format(@"Provider=VFPOLEDB.1;Data Source= {0} ", Directorio);
                                string TipoCondicion = dtw[1]["TipoCondicion"].ToString();
                                string TipoCarga = dtw[1]["TipoCarga"].ToString();
                                string condicion = "";
                                switch (TipoCondicion)
                                {
                                    case "F":
                                        //cuando el campo es date
                                        condicion = string.Format(" WHERE !Deleted() and  {0} > Ctod('{1}') ", columnaCondicion, ValorCondicion);
                                        break;
                                    case "E":
                                        //cuando el campo es dateTime
                                        condicion = string.Format(" WHERE !Deleted() and  {0} > {1} ", columnaCondicion, ValorCondicion);
                                        break;
                                    default:
                                        condicion = "";
                                        break;
                                }

                                tareas.Add(Task.Factory.StartNew(() => ConvertDbf(connectionString2, dbfToConvert2, sql2, conn, TablaDestino, condicion, sucursal, columnaCondicion,TipoCarga)));
                               // tarea2 = Task.Factory.StartNew(() => ConvertDbf(connectionString2, dbfToConvert2, sql2, conn, TablaDestino,condicion));
                                //Thread hilo2 = new Thread(delegate () { ConvertDbf1(connectionString2, dbfToConvert2, sql2, conn, TablaDestino); });
                                //hilo2.Start();
                            }
                            if (dtw.Count() > 2)
                            {
                                string sql2 = dtw[2]["SelectTable"].ToString();
                                string TablaDestino = dtw[2]["TablaDestino"].ToString();
                                string NombreArchivo = dtw[2]["NombreTabla"].ToString();
                                TablaExcepcion = dtw[2]["NombreTabla"].ToString();
                                 Directorio = dtw[2]["Directorio"].ToString();
                                string sucursal = dtw[2]["sucursal"].ToString();
                                sql2 = sql2 + string.Format(" ,'{0}' as sucursal", sucursal);
                                string columnaCondicion = dtw[2]["ColumnaCondicion"].ToString();
                                string ValorCondicion = dtw[2]["ValorCondicion"].ToString();
                                string dbfToConvert2 = string.Concat(Directorio, NombreArchivo);
                                string connectionString2 = string.Format(@"Provider=VFPOLEDB.1;Data Source= {0} ", Directorio);
                                string TipoCondicion = dtw[2]["TipoCondicion"].ToString();
                                string TipoCarga = dtw[2]["TipoCarga"].ToString();
                                string condicion = "";
                                switch (TipoCondicion)
                                {
                                    case "F":
                                        //cuando el campo es date
                                        condicion = string.Format(" WHERE !Deleted() and  {0} > Ctod('{1}') ", columnaCondicion, ValorCondicion);
                                        break;
                                    case "E":
                                        //cuando el campo es dateTime
                                        condicion = string.Format(" WHERE !Deleted() and  {0} > {1} ", columnaCondicion, ValorCondicion);
                                        break;
                                    default:
                                        condicion = "";
                                        break;
                                }

                                tareas.Add(Task.Factory.StartNew(() => ConvertDbf(connectionString2, dbfToConvert2, sql2, conn, TablaDestino, condicion, sucursal, columnaCondicion,TipoCarga)));
                                //tarea3 = Task.Factory.StartNew(() => ConvertDbf(connectionString2, dbfToConvert2, sql2, conn, TablaDestino,condicion));
                                //Thread hilo2 = new Thread(delegate () { ConvertDbf1(connectionString2, dbfToConvert2, sql2, conn, TablaDestino); });
                                //hilo2.Start();
                            }
                            if (dtw.Count() > 3)
                            {
                                string sql3 = dtw[3]["SelectTable"].ToString();
                                string TablaDestino = dtw[3]["TablaDestino"].ToString();
                                string NombreArchivo = dtw[3]["NombreTabla"].ToString();
                                TablaExcepcion = dtw[3]["NombreTabla"].ToString();
                                Directorio = dtw[3]["Directorio"].ToString();
                                string sucursal = dtw[3]["sucursal"].ToString();
                                sql3 = sql3 + string.Format(" ,'{0}' as sucursal", sucursal);
                                string columnaCondicion = dtw[3]["ColumnaCondicion"].ToString();
                                string ValorCondicion = dtw[3]["ValorCondicion"].ToString();
                                string dbfToConvert3 = string.Concat(Directorio, NombreArchivo);
                                string connectionString2 = string.Format(@"Provider=VFPOLEDB.1;Data Source= {0} ", Directorio);
                                string TipoCondicion = dtw[3]["TipoCondicion"].ToString();
                                string TipoCarga = dtw[3]["TipoCarga"].ToString();
                                string condicion = "";
                                switch (TipoCondicion)
                                {
                                    case "F":
                                        //cuando el campo es date
                                        condicion = string.Format(" WHERE !Deleted() and  {0} > Ctod('{1}') ", columnaCondicion, ValorCondicion);
                                        break;
                                    case "E":
                                        //cuando el campo es dateTime
                                        condicion = string.Format(" WHERE !Deleted() and  {0} > {1} ", columnaCondicion, ValorCondicion);
                                        break;
                                    default:
                                        condicion = "";
                                        break;
                                }


                                tareas.Add(Task.Factory.StartNew(() => ConvertDbf(connectionString2, dbfToConvert3, sql3, conn, TablaDestino, condicion, sucursal, columnaCondicion, TipoCarga)));
                                //tarea4 = Task.Factory.StartNew(() => ConvertDbf(connectionString2, dbfToConvert3, sql3, conn, TablaDestino,condicion));
                                //Thread hilo2 = new Thread(delegate () { ConvertDbf1(connectionString2, dbfToConvert2, sql2, conn, TablaDestino); });
                                //hilo2.Start();
                            }
                            if (dtw.Count() > 4)
                            {
                                string sql4 = dtw[4]["SelectTable"].ToString();
                                string TablaDestino = dtw[4]["TablaDestino"].ToString();
                                string NombreArchivo = dtw[4]["NombreTabla"].ToString();
                                TablaExcepcion = dtw[4]["NombreTabla"].ToString();
                                Directorio = dtw[4]["Directorio"].ToString();
                                string sucursal = dtw[4]["sucursal"].ToString();
                                sql4 = sql4 + string.Format(" ,'{0}' as sucursal", sucursal);
                                string columnaCondicion = dtw[4]["ColumnaCondicion"].ToString();
                                string ValorCondicion = dtw[4]["ValorCondicion"].ToString();
                                string dbfToConvert4 = string.Concat(Directorio, NombreArchivo);
                                // condicion = string.Format(" WHERE {0} > Ctod('{1}') ", columnaCondicion, ValorCondicion);
                                string connectionString2 = string.Format(@"Provider=VFPOLEDB.1;Data Source= {0} ", Directorio);
                                string TipoCondicion = dtw[4]["TipoCondicion"].ToString();
                                string TipoCarga = dtw[4]["TipoCarga"].ToString();
                                string condicion = "";
                                switch (TipoCondicion)
                                {
                                    case "F":
                                        //cuando el campo es date
                                        condicion = string.Format(" WHERE !Deleted() and   {0} > Ctod('{1}') ", columnaCondicion, ValorCondicion);
                                        break;
                                    case "E":
                                        //cuando el campo es dateTime
                                        condicion = string.Format(" WHERE !Deleted() and  {0} > {1} ", columnaCondicion, ValorCondicion);
                                        break;
                                    default:
                                        condicion = "";
                                        break;
                                }

                                tareas.Add(Task.Factory.StartNew(() => ConvertDbf(connectionString2, dbfToConvert4, sql4, conn, TablaDestino, condicion, sucursal, columnaCondicion, TipoCarga)));
                                //tarea5 = Task.Factory.StartNew(() => ConvertDbf(connectionString2, dbfToConvert4, sql4, conn, TablaDestino,condicion));
                                //Thread hilo2 = new Thread(delegate () { ConvertDbf1(connectionString2, dbfToConvert2, sql2, conn, TablaDestino); });
                                //hilo2.Start();
                            }
                            if (dtw.Count() > 5)
                            {
                                string sql5 = dtw[5]["SelectTable"].ToString();
                                string TablaDestino = dtw[5]["TablaDestino"].ToString();
                                string NombreArchivo = dtw[5]["NombreTabla"].ToString();
                                TablaExcepcion = dtw[5]["NombreTabla"].ToString();
                                Directorio = dtw[5]["Directorio"].ToString();
                                string sucursal = dtw[5]["sucursal"].ToString();
                                sql5 = sql5 + string.Format(" ,'{0}' as sucursal", sucursal);
                                string columnaCondicion = dtw[5]["ColumnaCondicion"].ToString();
                                string ValorCondicion = dtw[5]["ValorCondicion"].ToString();
                                string dbfToConvert5 = string.Concat(Directorio, NombreArchivo);
                                string connectionString2 = string.Format(@"Provider=VFPOLEDB.1;Data Source= {0} ", Directorio);
                                string TipoCondicion = dtw[5]["TipoCondicion"].ToString();
                                string TipoCarga = dtw[5]["TipoCarga"].ToString();
                                string condicion = "";
                                switch (TipoCondicion)
                                {
                                    case "F":
                                        //cuando el campo es date
                                        condicion = string.Format(" WHERE !Deleted() and  {0} > Ctod('{1}') ", columnaCondicion, ValorCondicion);
                                        break;
                                    case "E":
                                        //cuando el campo es dateTime
                                        condicion = string.Format(" WHERE !Deleted() and  {0} > {1} ", columnaCondicion, ValorCondicion);
                                        break;
                                    default:
                                        condicion = "";
                                        break;
                                }

                                tareas.Add(Task.Factory.StartNew(() => ConvertDbf(connectionString2, dbfToConvert5, sql5, conn, TablaDestino, condicion, sucursal, columnaCondicion, TipoCarga)));

                            }
                            if (dtw.Count() > 6)
                            {
                                string sql6 = dtw[6]["SelectTable"].ToString();
                                string TablaDestino = dtw[6]["TablaDestino"].ToString();
                                string NombreArchivo = dtw[6]["NombreTabla"].ToString();
                                TablaExcepcion = dtw[6]["NombreTabla"].ToString();
                                Directorio = dtw[6]["Directorio"].ToString();
                                string sucursal = dtw[6]["sucursal"].ToString();
                                sql6 = sql6 + string.Format(" ,'{0}' as sucursal", sucursal);
                                string columnaCondicion = dtw[6]["ColumnaCondicion"].ToString();
                                string ValorCondicion = dtw[6]["ValorCondicion"].ToString();
                                string dbfToConvert6 = string.Concat(Directorio, NombreArchivo);
                                string connectionString2 = string.Format(@"Provider=VFPOLEDB.1;Data Source= {0} ", Directorio);
                                string TipoCondicion = dtw[6]["TipoCondicion"].ToString();
                                string TipoCarga = dtw[6]["TipoCarga"].ToString();
                                string condicion = "";
                                switch (TipoCondicion)
                                {
                                    case "F":
                                        //cuando el campo es date
                                        condicion = string.Format(" WHERE !Deleted() and  {0} > Ctod('{1}') ", columnaCondicion, ValorCondicion);
                                        break;
                                    case "E":
                                        //cuando el campo es dateTime
                                        condicion = string.Format(" WHERE !Deleted() and  {0} > {1} ", columnaCondicion, ValorCondicion);
                                        break;
                                    default:
                                        condicion = "";
                                        break;
                                }

                                tareas.Add(Task.Factory.StartNew(() => ConvertDbf(connectionString2, dbfToConvert6, sql6, conn, TablaDestino, condicion, sucursal, columnaCondicion, TipoCarga)));

                            }

                            Task.WaitAll(tareas.ToArray());                         

                            numero += 7 ;

                        }


                    }

                }

            }
            catch (Exception ex)
            {
                EscribirLog(ex.Message + "  main" , dbfPath, TablaExcepcion);
                Console.WriteLine(ex.Message);
            }


  
        }
        public static  bool FileIsLocked(string strFullFileName)
        {
            bool blnReturn = false;
            FileStream fs;
            try
            {
                fs = System.IO.File.Open(strFullFileName, FileMode.OpenOrCreate, FileAccess.Read, FileShare.None);
                fs.Close();
            }
            catch (System.IO.IOException ex)
            {
                blnReturn = true;
            }
            return blnReturn;
        }
        static void ConvertDbf(string connectionString, string dbfFile, string comando,string sqlcon,string tablaDestino,string condicion,string sucursal,string columnaCondicion,string TipoCarga)
        {
            try
            {
                DataTable dtr = new DataTable();
                string CondicionValor = string.IsNullOrEmpty(condicion) ? " WHERE !Deleted() " : condicion;

               

                string sqlSelect = string.Format(comando + " FROM {0} {1}  ", dbfFile, CondicionValor);
                //string sqlSelect = string.Format("SELECT CAST(NUMTIENDA AS VARCHAR(200)) AS [NUMTIENDA] FROM {0}   ", dbfFile);
               // DataTable DtSquema = new DataTable();
               // string[] TablaRestrinccion = new string[4];
               // TablaRestrinccion[2] = "INV_DOCUMENTOS.dbf";
                using (OleDbConnection connection = new OleDbConnection(connectionString))
                {
                    connection.Open();
                   //DtSquema = connection.GetSchema("Columns", TablaRestrinccion);
                    

                    using (OleDbDataAdapter da = new OleDbDataAdapter(sqlSelect, connection))
                    {
                        DataSet ds = new DataSet();
                        Console.WriteLine(DateTime.Now + " ==> iniciando Lectura archivo dbf  " + dbfFile + "  ==>  " );
                        da.Fill(dtr);
                        Console.WriteLine(DateTime.Now + " ==> Finalizo Lectura archivo dbf  " + dbfFile + "  ==>  ");


                        using (SqlConnection conexion = new SqlConnection(sqlcon))
                        {
                            conexion.Open();
                            using (SqlBulkCopy bulkcopy = new SqlBulkCopy(conexion))
                            {

                                bulkcopy.DestinationTableName = tablaDestino;
                                bulkcopy.BulkCopyTimeout = 0;
                                try
                                {
                                    Console.WriteLine(DateTime.Now + " ==> iniciando Carga AzSQL  " + tablaDestino + "  ==>  ");
                                    bulkcopy.WriteToServer(dtr);


                                    ActualizarTablaConfig(tablaDestino, sucursal, sqlcon, dtr, columnaCondicion);

                                    Console.WriteLine(DateTime.Now + " ==> finalizo Carga AzSQL  " +  tablaDestino + "  ==>  " + dbfFile );

                                }
                                catch (Exception ex)
                                {
                                                                       
                                    EscribirLog(ex.Message + "  bulk", dbfPath +"  "+ dbfFile, tablaDestino);
                                    //Dts.TaskResult = (int)ScriptResults.Failure;
                                    // MessageBox.Show(" en el bulk " + ex.Message);
                                }

                            }
                            conexion.Close();
                        }
                        //DataTableToCSV(ds.Tables[0], csvFile);
                    }
                    connection.Close();
                }
            }
            catch (Exception ex)
            {
                EscribirLog(ex.Message + "  ConvertDbf", dbfPath, dbfFile);
            }
        }
        static void ActualizarTablaConfig( string tablaDestino, string sucursal, string sqlcon,DataTable dtarc,string columnaCondicion)
        {
            try
            {
                //DataTable dtr = new DataTable();
                //string CondicionValor = string.IsNullOrEmpty(condicion) ? " WHERE !Deleted() " : condicion;
                //

                if (columnaCondicion != "")
                {
                    var Fmin = dtarc.AsEnumerable().Max(x => x.Field<string>(columnaCondicion));

                    string sqlSelect = string.Format("update dbo.tablas set ValorCondicion ='{0}' WHERE TablaDestino ='{1}'  and sucursal ='{2}' ", Fmin, tablaDestino, sucursal); //stg.TablaConfiguracion -- dbo.tablas
                                                                                                                                                                                                //string sqlSelect = string.Format("SELECT CAST(NUMTIENDA AS VARCHAR(200)) AS [NUMTIENDA] FROM {0}   ", dbfFile);
                    DataTable DtSquema = new DataTable();

                    if (Fmin != null)
                    {
                        using (SqlConnection conexion = new SqlConnection(sqlcon))
                        {
                            conexion.Open();

                            using (SqlCommand cmd = new SqlCommand(sqlSelect, conexion))
                            {
                                cmd.ExecuteNonQuery();
                            }


                        }
                    }
                }
                //DataTableToCSV(ds.Tables[0], csvFile);                  

            }
            catch (Exception ex)
            {
                EscribirLog(ex.Message + "  actualizaCol", dbfPath, tablaDestino);
            }
        }
               
        static void DataTableToCSV(DataTable dt, string csvFile)
        {
            StringBuilder sb = new StringBuilder();
            var columnNames = dt.Columns.Cast<DataColumn>().Select(column => column.ColumnName).ToArray();
            sb.AppendLine(string.Join(",", columnNames));
            foreach (DataRow row in dt.Rows)
            {
                var fields = row.ItemArray.Select(field => field.ToString()).ToArray();
                for (int i = 0; i < fields.Length; i++)
                {
                    sb.Append("\"" + fields[i].Trim());
                    sb.Append((i != fields.Length - 1) ? "\"," : "\"");
                }
                sb.Append("\r\n");
            }
            File.WriteAllText(csvFile, sb.ToString());
        }
        public static void CargaFull(string archivo, string tablaDestino,string conexion)
        {
            var options = new DbfDataReaderOptions
            {
                SkipDeletedRecords = true
                                          // Encoding = EncodingProvider.GetEncoding(1252);
            };

            using (DbfDataReader.DbfDataReader dbfre = new DbfDataReader.DbfDataReader(archivo, options))
            {
                using (SqlBulkCopy bulkcopy = new SqlBulkCopy(conexion))
                {
                    


                    bulkcopy.DestinationTableName = tablaDestino;//"[dbo].INV_DEPARTAMENTOS";
                    bulkcopy.BulkCopyTimeout = 0;
                    try
                    {
                        bulkcopy.WriteToServer(dbfre);
                    }
                    catch (Exception ex)
                    {
                        //Dts.TaskResult = (int)ScriptResults.Failure;
                        // MessageBox.Show(" en el bulk " + ex.Message);
                    }

                }
            }
        }
        public static void EscribirLog(string mensaje, string ruta,string archivo)
        {
            // Set Status to Locked
            _readWriteLock.EnterWriteLock();
            try
            {
                // Append text to the file
                using (StreamWriter sw = File.AppendText($@"{ruta.Trim()}log.txt"))
                {
                    sw.Write("\r\nRegistro de Log : ");
                    sw.WriteLine($"{DateTime.Now.ToLongTimeString()} {DateTime.Now.ToLongDateString()}");
                    sw.Write("\r\nArchivo : ");
                    sw.WriteLine($"  :{archivo}");
                    sw.WriteLine("  :");
                    sw.WriteLine($"  :{mensaje}");
                    sw.WriteLine("-------------------------------");                   
                    sw.Close();
                }
            }
            finally
            {
                // Release lock
                _readWriteLock.ExitWriteLock();
            }
           
        }
        static void TruncateTableFullCarga(string tabla,  string conexion )
        {
            try
            {
                string sql = string.Format("truncate table {0}", tabla);

                using (SqlConnection connection = new SqlConnection(conexion))
                {
                    connection.Open();
                    using (SqlCommand cmd = new SqlCommand(sql, connection))
                    {
                        cmd.ExecuteNonQuery();

                    }
                }
               
            }
            catch (Exception ex)
            {
                EscribirLog(ex.Message + "  TruncateTA", dbfPath, tabla);
            }
            
        }
    }


}
