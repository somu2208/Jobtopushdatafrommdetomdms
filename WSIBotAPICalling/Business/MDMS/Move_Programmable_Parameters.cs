using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using System.Configuration;
using System.Data.SqlClient;
using System.Data;

namespace SMDataMSSQLtoMySQLWJ.MDMS
{
    class Move_Programmable_Parameters
    {
        static MySqlConnection mysqlcnn = new MySqlConnection(ConfigurationManager.ConnectionStrings["MySQlMDMSConnection"].ToString());
        static SqlConnection mssqlcnn = new SqlConnection(ConfigurationManager.ConnectionStrings["MSSQlMDASConnection"].ToString());

        public Move_Programmable_Parameters()
        {
            try
            {
                SqlDataAdapter DAcmd = new SqlDataAdapter("select * from profiles_datamdms_sync_info where profile_Type='programmable_parameters_' and [read]='1'", mssqlcnn);
                DataSet ds = new DataSet();
                DAcmd.Fill(ds);
                if (ds != null && ds.Tables.Count > 0 && ds.Tables[0].Rows.Count > 0)
                {
                    SqlDataAdapter DAcmd2 = new SqlDataAdapter("select * from " + ds.Tables[0].Rows[0][1].ToString() + "values where server_time > '" + Convert.ToDateTime(ds.Tables[0].Rows[0][2].ToString()).ToString("yyyy-MM-dd HH:mm:ss") + "' order by server_time asc", mssqlcnn);
                    DataSet ds2 = new DataSet();
                    DAcmd2.Fill(ds2);
                    if (ds2 != null && ds2.Tables.Count > 0 && ds2.Tables[0].Rows.Count > 0)
                    {
                        for (int k = 0; k < ds2.Tables[0].Rows.Count; k++)
                        {
                            string MeterID = ds2.Tables[0].Rows[k]["meter_id"].ToString();
                            //Get Meter Name and Accesskey
                            SqlCommand cmd = new SqlCommand("select * from meter_configuration where id='" + MeterID + "'", mssqlcnn);
                            if (mssqlcnn.State == ConnectionState.Closed)
                            {
                                mssqlcnn.Open();
                            }
                            SqlDataReader dr = cmd.ExecuteReader();
                            string MeterName = "";
                            string AccessKey = "";
                            string MeterTypeID = "";
                            if (dr.Read())
                            {
                                MeterName = dr["serial_number"].ToString();
                                AccessKey = dr["meter_access_key"].ToString();
                                MeterTypeID = dr["meter_template_type"].ToString();
                            }
                            dr.Close();
                            if (MeterName != "")
                            {
                                string Result = "";
                                SqlDataAdapter DAcmd3 = new SqlDataAdapter("select * from " + ds.Tables[0].Rows[0][1].ToString() + "master where meter_type_id = '" + MeterTypeID + "'", mssqlcnn);
                                DataSet ds3 = new DataSet();
                                DAcmd3.Fill(ds3);
                                if (ds3 != null && ds3.Tables.Count > 0 && ds3.Tables[0].Rows.Count > 0)
                                {
                                    SupportingModules.SupportingMethods ObjSM = new SupportingModules.SupportingMethods();
                                    string UtilityID = ObjSM.GetUtilityID(MeterName);
                                    Result = InsertSinglePhaseProgrammableParameters(ds2.Tables[0].Rows[k], MeterName, UtilityID);                                    
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                string error = ex.Message;
                Logs.Logging_Transaction message_log = new Logs.Logging_Transaction("Programmable_Parameers_Main", ex.Message);
            }
            finally
            {
                mssqlcnn.Close();
            }
        }

        static string InsertSinglePhaseProgrammableParameters(DataRow InstRow, string serial_number,string UtilityID)
        {
            try
            {
                DateTime dtSource_Time = Convert.ToDateTime(InstRow["source_time_stamp"]);
                DateTime dtServerTime = Convert.ToDateTime(InstRow["server_time"]);
                string QueryString = "INSERT INTO meter_parameters_single_phase VALUES(";
                QueryString = QueryString + "'" + serial_number + "',";
                QueryString = QueryString + "'" + dtSource_Time.ToString("yyyy-MM-dd HH:mm:ss") + "',";
                QueryString = QueryString + "'" + dtServerTime.ToString("yyyy-MM-dd HH:mm:ss") + "',";
                if (InstRow["P02"]!=null && InstRow["P02"].ToString()!="")
                {
                    QueryString = QueryString + "'" + InstRow["P02"] + "',";
                }
                else
                {
                    QueryString = QueryString + "NULL,";
                }
                if (InstRow["P03"] != null && InstRow["P03"].ToString() != "")
                {
                    QueryString = QueryString + "'" + InstRow["P03"] + "',";
                }
                else
                {
                    QueryString = QueryString + "NULL,";
                }
                if (InstRow["P04"] != null && InstRow["P04"].ToString() != "")
                {
                    QueryString = QueryString + "'" + InstRow["P04"] + "',";
                }
                else
                {
                    QueryString = QueryString + "NULL,";
                }
                if (InstRow["P05"] != null && InstRow["P05"].ToString() != "")
                {
                    QueryString = QueryString + "'" + InstRow["P05"] + "',";
                }
                else
                {
                    QueryString = QueryString + "NULL,";
                }
                if (InstRow["P06"] != null && InstRow["P06"].ToString() != "")
                {
                    QueryString = QueryString + "'" + InstRow["P06"] + "',";
                }
                else
                {
                    QueryString = QueryString + "NULL,";
                }
                if (InstRow["P07"] != null && InstRow["P07"].ToString() != "")
                {
                    QueryString = QueryString + "'" + InstRow["P07"] + "',";
                }
                else
                {
                    QueryString = QueryString + "NULL,";
                }
                if (InstRow["P08"] != null && InstRow["P08"].ToString() != "")
                {
                    QueryString = QueryString + "'" + InstRow["P08"] + "',";
                }
                else
                {
                    QueryString = QueryString + "NULL,";
                }
                if (InstRow["P09"] != null && InstRow["P09"].ToString() != "")
                {
                    QueryString = QueryString + "'" + InstRow["P09"] + "',";
                }
                else
                {
                    QueryString = QueryString + "NULL,";
                }
                if (InstRow["P10"] != null && InstRow["P10"].ToString() != "")
                {
                    QueryString = QueryString + "'" + InstRow["P10"] + "',";
                }
                else
                {
                    QueryString = QueryString + "NULL,";
                }
                if (InstRow["P11"] != null && InstRow["P11"].ToString() != "")
                {
                    QueryString = QueryString + "'" + InstRow["P11"] + "',";
                }
                else
                {
                    QueryString = QueryString + "NULL,";
                }
                if (InstRow["P12"] != null && InstRow["P12"].ToString() != "")
                {
                    QueryString = QueryString + "'" + InstRow["P12"] + "',";
                }
                else
                {
                    QueryString = QueryString + "NULL,";
                }
                if (InstRow["P13"] != null && InstRow["P13"].ToString() != "")
                {
                    QueryString = QueryString + "'" + InstRow["P13"] + "',";
                }
                else
                {
                    QueryString = QueryString + "NULL,";
                }
                if (InstRow["P14"] != null && InstRow["P14"].ToString() != "")
                {
                    QueryString = QueryString + "'" + InstRow["P14"] + "',";
                }
                else
                {
                    QueryString = QueryString + "NULL,";
                }
                if (InstRow["P15"] != null && InstRow["P15"].ToString() != "")
                {
                    QueryString = QueryString + "'" + InstRow["P15"] + "',";
                }
                else
                {
                    QueryString = QueryString + "NULL,";
                }
                if (InstRow["P16"] != null && InstRow["P16"].ToString() != "")
                {
                    QueryString = QueryString + "'" + InstRow["P16"] + "',";
                }
                else
                {
                    QueryString = QueryString + "NULL,";
                }
                if (InstRow["P17"] != null && InstRow["P17"].ToString() != "")
                {
                    QueryString = QueryString + "'" + InstRow["P17"] + "',";
                }
                else
                {
                    QueryString = QueryString + "NULL,";
                }
                if (InstRow["P18"] != null && InstRow["P18"].ToString() != "")
                {
                    QueryString = QueryString + "'" + InstRow["P18"] + "',";
                }
                else
                {
                    QueryString = QueryString + "NULL,";
                }
                if (InstRow["P19"] != null && InstRow["P19"].ToString() != "")
                {
                    QueryString = QueryString + "'" + InstRow["P19"] + "',";
                }
                else
                {
                    QueryString = QueryString + "NULL,";
                }
                if (InstRow["P20"] != null && InstRow["P20"].ToString() != "")
                {
                    QueryString = QueryString + "'" + InstRow["P20"] + "',";
                }
                else
                {
                    QueryString = QueryString + "NULL,";
                }
                if (InstRow["P21"] != null && InstRow["P21"].ToString() != "")
                {
                    QueryString = QueryString + "'" + InstRow["P21"] + "',";
                }
                else
                {
                    QueryString = QueryString + "NULL,";
                }
                //if (InstRow["P22"] != null && InstRow["P22"].ToString() != "")
                //{
                //    QueryString = QueryString + "'" + InstRow["P22"] + "',";
                //}
                //else
                //{
                //    QueryString = QueryString + "NULL,";
                //}
                //if (InstRow["P23"] != null && InstRow["P23"].ToString() != "")
                //{
                //    QueryString = QueryString + "'" + InstRow["P23"] + "',";
                //}
                //else
                //{
                //    QueryString = QueryString + "NULL,";
                //}
                //if (InstRow["P24"] != null && InstRow["P24"].ToString() != "")
                //{
                //    QueryString = QueryString + "'" + InstRow["P24"] + "',";
                //}
                //else
                //{
                //    QueryString = QueryString + "NULL,";
                //}
                //if (InstRow["P25"] != null && InstRow["P25"].ToString() != "")
                //{
                //    QueryString = QueryString + "'" + InstRow["P25"] + "',";
                //}
                //else
                //{
                //    QueryString = QueryString + "NULL,";
                //}
                //if (InstRow["P26"] != null && InstRow["P26"].ToString() != "")
                //{
                //    QueryString = QueryString + "'" + InstRow["P26"] + "',";
                //}
                //else
                //{
                //    QueryString = QueryString + "NULL,";
                //}

                QueryString = QueryString + UtilityID + ")";

                MySqlCommand cmd = new MySqlCommand(QueryString, mysqlcnn);
                if (mysqlcnn.State == System.Data.ConnectionState.Closed)
                {
                    mysqlcnn.Open();
                }
                int k = cmd.ExecuteNonQuery();
                if (k > 0)
                {
                    UpdateSyncinfo(dtServerTime, "programmable_parameters_");
                    return "Success";
                }
                else
                {
                    Logs.Logging_Transaction message_log = new Logs.Logging_Transaction("Programmable_Parameers", "Fail");
                    return "Fail";
                }
            }
            catch (Exception ex)
            {
                return ex.Message;
            }
            finally
            {
                mysqlcnn.Close();
            }
        }

        static void UpdateSyncinfo(DateTime ProfileDate, string ProfileName)
        {
            try
            {
                SqlCommand cmd = new SqlCommand("update profiles_datamdms_sync_info set sync_Date='" + ProfileDate.ToString("yyyy-MM-dd HH:mm:ss") + "' where profile_Type='" + ProfileName + "'", mssqlcnn);
                if (mssqlcnn.State == ConnectionState.Closed)
                {
                    mssqlcnn.Open();
                }
                int u = cmd.ExecuteNonQuery();
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
