using System;
using DbfDataReader;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
namespace ImportBDFfile
{
    class Program
    {
        static void Main(string[] args)
        {

            string conn = "Data Source=MSI-1387; Initial Catalog=FARMATRIX; Connection Timeout=0;Connection Lifetime=0;Min Pool Size=0;Max Pool Size=1000;Pooling=true ; Integrated Security=true;";
            var dbfPath = @"C:\Conexion\INV_DEPARTAMENTOS.DBF";

            //to iterate over the rows:
            var skipDeleted = true;


            var options = new DbfDataReaderOptions
            {
                SkipDeletedRecords = true
                // Encoding = EncodingProvider.GetEncoding(1252);
            };
           // int cuenta = 0;
            using (DbfDataReader.DbfDataReader dbfre = new DbfDataReader.DbfDataReader(dbfPath, options))
            {
                using (SqlBulkCopy bulkcopy = new SqlBulkCopy(conn))
                {
                    bulkcopy.DestinationTableName = "[dbo].INV_DEPARTAMENTOS";
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

           





                // There is also an implementation of DbDataReader:
                //using ( DbfDataReader.DbfDataReader dbfre  = new DbfDataReader.DbfDataReader(dbfPath, options))
                //{
                //    while (dbfre.Read())
                //    {
                //        var valueCol1 = dbfre.GetInt32(0);//dbfre.GetString(0);
                //        //var valueCol2 = dbfre.GetDecimal(1);
                //        //var valueCol3 = dbfre.GetDateTime(2);
                //        //var valueCol4 = dbfre.GetInt32(3);
                //    }
                //}


                //to iterate over the rows:

                ////var dbfPath = "path/file.dbf";
                //using (var dbfTable = new DbfTable(dbfPath, Encoding.UTF8))
                //{
                //    var dbfRecord = new DbfRecord(dbfTable);

                //    while (dbfTable.Read(dbfRecord))
                //    {
                //        if (skipDeleted && dbfRecord.IsDeleted)
                //        {
                //            continue;
                //        }

                //        foreach (var dbfValue in dbfRecord.Values)
                //        {
                //            //var stringValue = dbfValue.ToString();
                //            //var obj = dbfValue.GetValue();
                //            Console.WriteLine(dbfValue.ToString());
                //        }
                //    }
                //}



                Console.ReadKey();
            //Usage, to get summary info: ;
            //using (DbfTable dbt = new DbfTable(dbfPath, Encoding.UTF8))
            //{
            //    var header = dbt.Header;


            //    var versionDescription = header.VersionDescription;
            //    var hasMemo = dbt.Memo != null;
            //    var recordCount = header.RecordCount;

            //    foreach (var dbfColumn in dbt.Columns)
            //    {
            //        var name = dbfColumn.ColumnName;
            //        var columnType = dbfColumn.ColumnType;
            //        var length = dbfColumn.Length;
            //        var decimalCount = dbfColumn.DecimalCount;
            //    }
            //}


        }
    }
}
