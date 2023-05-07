using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using System.Configuration;
using System.Data.SqlClient;
using System.Data;

namespace SMDataMSSQLtoMySQLWJ.Logs
{
    public class Logging_Transaction
    {
        static SqlConnection mssqlcnn = new SqlConnection(ConfigurationManager.ConnectionStrings["MSSQlMDASConnection"].ToString());
        public Logging_Transaction(string MeterID, string Msg)
        {
            MessgeLog(MeterID, Msg);
        }

        public static void MessgeLog(string MeterID, string Msg)
        {
            try
            {
                SqlCommand cmd1 = new SqlCommand(@"insert into T1034(F02,F03,F04) values('" + MeterID + "','" + Msg.Replace("'","") + "','" + DateTime.UtcNow.ToString("MM-dd-yyyy HH:mm:ss") + "')", mssqlcnn);
                if (mssqlcnn.State == ConnectionState.Closed)
                {
                    mssqlcnn.Open();
                }
                int k = cmd1.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                string Error = ex.Message;
            }
            finally
            {
                mssqlcnn.Close();
            }
        }
    }
}
