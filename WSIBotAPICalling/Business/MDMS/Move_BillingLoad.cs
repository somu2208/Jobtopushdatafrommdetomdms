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
    class Move_BillingLoad
    {
        static MySqlConnection mysqlcnn = new MySqlConnection(ConfigurationManager.ConnectionStrings["MySQlMDMSConnection"].ToString());
        static SqlConnection mssqlcnn = new SqlConnection(ConfigurationManager.ConnectionStrings["MSSQlMDASConnection"].ToString());
        public Move_BillingLoad()
        {
            try
            {
                SqlDataAdapter DAcmd = new SqlDataAdapter("select * from profiles_datamdms_sync_info where profile_Type='billing_profile_' and [read]='1'", mssqlcnn);
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
                                    if (ds3.Tables[0].Rows[0]["meter_type_id"].ToString() == "1006")
                                    {
                                        Result = InsertSinglePhaseBillingLoad(ds2.Tables[0].Rows[k], MeterName, UtilityID);
                                    }
                                    else if (ds3.Tables[0].Rows[0]["meter_type_id"].ToString() == "1007")
                                    {
                                        Result = InsertThreePhaseBillingLoad(ds2.Tables[0].Rows[k], MeterName, UtilityID);
                                    }
                                    else if (ds3.Tables[0].Rows[0]["meter_type_id"].ToString() == "1012")
                                    {
                                        Result = InsertSinglePhaseBillingLoad(ds2.Tables[0].Rows[k], MeterName, UtilityID);
                                    }
                                    else if (ds3.Tables[0].Rows[0]["meter_type_id"].ToString() == "1015")
                                    {
                                        Result = InsertThreePhaseBillingLoad(ds2.Tables[0].Rows[k], MeterName, UtilityID);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Logs.Logging_Transaction message_log = new Logs.Logging_Transaction("Billing_Main", ex.Message);
            }
            finally
            {
                mssqlcnn.Close();
            }
        }

        static string InsertSinglePhaseBillingLoad(DataRow InstRow, string serial_number,string UtilityID)
        {
            try
            {
                DateTime dtSource_Time = Convert.ToDateTime(InstRow["source_time_stamp"]);
                DateTime dtServerTime = Convert.ToDateTime(InstRow["server_time"]);
                MySqlCommand cmd = new MySqlCommand(@"INSERT INTO meter_billing_profile_single_phase_new VALUES('" + serial_number + "','" + dtSource_Time.ToString("yyyy-MM-dd HH:mm:ss") + "','" + dtServerTime.ToString("yyyy-MM-dd HH:mm:ss") + "','" + InstRow["P02"] + "','" + InstRow["P03"] + "','" + InstRow["P04"] + "','" + InstRow["P05"] + "','" + InstRow["P06"] + "','" + InstRow["P07"] + "','" + InstRow["P08"] + "','" + InstRow["P09"] + "','" + InstRow["P10"] + "','" + InstRow["P11"] + "','" + InstRow["P12"] + "','" + InstRow["P13"] + "','" + InstRow["P14"] + "','" + InstRow["P15"] + "','" + InstRow["P16"] + "','" + InstRow["P17"] + "','" + InstRow["P18"] + "','" + InstRow["P19"] + "',NULL,NULL,'" + UtilityID + "',NULL,NULL)", mysqlcnn);
                if (mysqlcnn.State == System.Data.ConnectionState.Closed)
                {
                    mysqlcnn.Open();
                }
                int k = cmd.ExecuteNonQuery();
                if (k > 0)
                {
                    UpdateSyncinfo(dtServerTime, "billing_profile_");
                    Logs.Logging_Transaction message_log = new Logs.Logging_Transaction("Billing_1p", "Fail");
                    return "Success";
                }
                else
                {
                    return "Fail";
                }
            }
            catch (Exception ex)
            {
                Logs.Logging_Transaction message_log = new Logs.Logging_Transaction("Billing_1p", ex.Message);
                return ex.Message;
            }
            finally
            {
                mysqlcnn.Close();
            }
        }

        static string InsertThreePhaseBillingLoad(DataRow InstRow, string serial_number,string UtilityID)
        {
            try
            {
                DateTime dtSource_Time = Convert.ToDateTime(InstRow["source_time_stamp"]);
                DateTime dtServerTime = Convert.ToDateTime(InstRow["server_time"]);
                MySqlCommand cmd = new MySqlCommand(@"INSERT INTO meter_billing_profile_three_phase VALUES('" + serial_number + "','" + dtSource_Time.ToString("yyyy-MM-dd HH:mm:ss") + "','" + dtServerTime.ToString("yyyy-MM-dd HH:mm:ss") + "','" + InstRow["P02"] + "','" + InstRow["P03"] + "','" + InstRow["P04"] + "','" + InstRow["P05"] + "','" + InstRow["P06"] + "','" + InstRow["P07"] + "','" + InstRow["P08"] + "','" + InstRow["P09"] + "','" + InstRow["P10"] + "','" + InstRow["P11"] + "','" + InstRow["P12"] + "','" + InstRow["P13"] + "','" + InstRow["P14"] + "','" + InstRow["P15"] + "','" + InstRow["P16"] + "','" + InstRow["P17"] + "','" + InstRow["P18"] + "','" + InstRow["P19"] + "','" + InstRow["P20"] + "','" + InstRow["P21"] + "','" + InstRow["P22"] + "','" + InstRow["P23"] + "','" + InstRow["P24"] + "','" + InstRow["P25"] + "','" + InstRow["P26"] + "','" + InstRow["P27"] + "','" + InstRow["P28"] + "','" + InstRow["P29"] + "','" + InstRow["P30"] + "','" + InstRow["P31"] + "','" + InstRow["P32"] + "','" + InstRow["P33"] + "','" + InstRow["P34"] + "','" + InstRow["P35"] + "','" + InstRow["P36"] + "','" + InstRow["P37"] + "','" + InstRow["P38"] + "','" + InstRow["P39"] + "','" + InstRow["P40"] + "','" + InstRow["P41"] + "','" + InstRow["P42"] + "','" + InstRow["P43"] + "','" + InstRow["P44"] + "','" + InstRow["P45"] + "','" + InstRow["P46"] + "','" + InstRow["P47"] + "','" + InstRow["P48"] + "','" + InstRow["P49"] + "','" + InstRow["P50"] + "','" + InstRow["P51"] + "','" + InstRow["P52"] + "','" + InstRow["P53"] + "','" + InstRow["P54"] + "','" + InstRow["P55"] + "','" + InstRow["P56"] + "','" + InstRow["P57"] + "','" + InstRow["P58"] + "','" + InstRow["P59"] + "','" + InstRow["P60"] + "','" + InstRow["P61"] + "','" + InstRow["P62"] + "','" + InstRow["P63"] + "','" + InstRow["P64"] + "','" + InstRow["P65"] + "','" + InstRow["P66"] + "','" + InstRow["P67"] + "','" + InstRow["P68"] + "','" + InstRow["P69"] + "','" + InstRow["P70"] + "','" + InstRow["P71"] + "','" + InstRow["P72"] + "','" + InstRow["P73"] + "','" + InstRow["P74"] + "','" + InstRow["P75"] + "','" + InstRow["P76"] + "','" + InstRow["P77"] + "','" + InstRow["P78"] + "','" + InstRow["P79"] + "',NULL,NULL,'" + UtilityID + "',NULL,NULL)", mysqlcnn);
                if (mysqlcnn.State == System.Data.ConnectionState.Closed)
                {
                    mysqlcnn.Open();
                }
                int k = cmd.ExecuteNonQuery();
                if (k > 0)
                {
                    UpdateSyncinfo(dtServerTime, "billing_profile_");
                    return "Success";
                }
                else
                {
                    Logs.Logging_Transaction message_log = new Logs.Logging_Transaction("Billing_3p", "Fail");
                    return "Fail";
                }
            }
            catch (Exception ex)
            {
                Logs.Logging_Transaction message_log = new Logs.Logging_Transaction("Billing_3p", ex.Message);
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
