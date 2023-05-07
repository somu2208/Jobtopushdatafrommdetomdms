using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using System.Configuration;
using System.Data.SqlClient;
using System.Data;

namespace SMDataMSSQLtoMySQLWJ.SupportingModules
{
    class SupportingMethods
    {
        static MySqlConnection mysqlcnn = new MySqlConnection(ConfigurationManager.ConnectionStrings["MySQlMDMSConnection"].ToString());
        //static SqlConnection mssqlcnn = new SqlConnection(ConfigurationManager.ConnectionStrings["MSSQlMDASConnection"].ToString());
        public SupportingMethods()
        {

        }

        public string GetUtilityID(string MeterID)
        {
            try
            {
                MySqlCommand cmd = new MySqlCommand("SELECT utility_id FROM meters WHERE meter_serial_number='" + MeterID + "'",mysqlcnn);
                if (mysqlcnn.State== ConnectionState.Closed)
                {
                    mysqlcnn.Open();
                }
                MySqlDataReader DR = cmd.ExecuteReader();
                if (DR.Read())
                {
                    return DR[0].ToString();
                }
                else
                {
                    return "NoData";
                }
            }
            catch (Exception ex)
            {
                return "Error";
            }
            finally
            {
                mysqlcnn.Close();
            }
        }
    }
}
