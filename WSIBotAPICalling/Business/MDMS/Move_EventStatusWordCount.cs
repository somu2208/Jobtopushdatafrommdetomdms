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
    class Move_EventStatusWordCount
    {
        static MySqlConnection mysqlcnn = new MySqlConnection(ConfigurationManager.ConnectionStrings["MySQlMDMSConnection"].ToString());
        static SqlConnection mssqlcnn = new SqlConnection(ConfigurationManager.ConnectionStrings["MSSQlMDASConnection"].ToString());
        public Move_EventStatusWordCount()
        {
            try
            {
                //SqlDataAdapter DAcmd = new SqlDataAdapter("select * from profiles_datamdms_sync_info where profile_Type='event_status_word_' and [read]='1'", mssqlcnn);
                //DataSet ds = new DataSet();
                //DAcmd.Fill(ds);
                //if (ds != null && ds.Tables.Count > 0 && ds.Tables[0].Rows.Count > 0)
                //{
                //    SqlDataAdapter DAcmd2 = new SqlDataAdapter("select * from " + ds.Tables[0].Rows[0][1].ToString() + "values where server_time > '" + Convert.ToDateTime(ds.Tables[0].Rows[0][2].ToString()).ToString("yyyy-MM-dd HH:mm:ss") + "' order by server_time asc", mssqlcnn);
                //    DataSet ds2 = new DataSet();
                //    DAcmd2.Fill(ds2);
                //    if (ds2 != null && ds2.Tables.Count > 0 && ds2.Tables[0].Rows.Count > 0)
                //    {
                //        for (int k = 0; k < ds2.Tables[0].Rows.Count; k++)
                //        {
                //            string MeterID = ds2.Tables[0].Rows[k]["meter_id"].ToString();
                //            //Get Meter Name and Accesskey
                //            SqlCommand cmd = new SqlCommand("select * from meter_configuration where id='" + MeterID + "'", mssqlcnn);
                //            if (mssqlcnn.State == ConnectionState.Closed)
                //            {
                //                mssqlcnn.Open();
                //            }
                //            SqlDataReader dr = cmd.ExecuteReader();
                //            string MeterName = "";
                //            string AccessKey = "";
                //            string MeterTypeID = "";
                //            if (dr.Read())
                //            {
                //                MeterName = dr["serial_number"].ToString();
                //                AccessKey = dr["meter_access_key"].ToString();
                //                MeterTypeID = dr["meter_template_type"].ToString();
                //            }
                //            dr.Close();
                //            if (MeterName != "")
                //            {
                //                string Result = "";
                //                SqlDataAdapter DAcmd3 = new SqlDataAdapter("select * from " + ds.Tables[0].Rows[0][1].ToString() + "master where meter_type_id = '" + MeterTypeID + "'", mssqlcnn);
                //                DataSet ds3 = new DataSet();
                //                DAcmd3.Fill(ds3);
                //                if (ds3 != null && ds3.Tables.Count > 0 && ds3.Tables[0].Rows.Count > 0)
                //                {
                //                    SupportingModules.SupportingMethods ObjSM = new SupportingModules.SupportingMethods();
                //                    string UtilityID = ObjSM.GetUtilityID(MeterName);
                //                    Result = Insertwordcountevents(ds2.Tables[0].Rows[k], MeterName, UtilityID);
                //                }
                //            }
                //        }
                //    }
                //}
            }
            catch (Exception ex)
            {
                Logs.Logging_Transaction message_log = new Logs.Logging_Transaction("Event_Status_Word", ex.Message);
            }
            finally
            {
                mssqlcnn.Close();
            }
        }

        static string Insertwordcountevents(DataRow InstRow, string serial_number,string UtilityID)
        {
            try
            {
                DateTime dtSource_Time = Convert.ToDateTime(InstRow["source_time_stamp"]);
                DateTime dtServerTime = Convert.ToDateTime(InstRow["server_time"]);
                if (InstRow["P04"]!=null && InstRow["P04"]!="")
                {
                    //find the availability of second latest record
                    SqlDataAdapter da = new SqlDataAdapter("select top 2 * from event_status_word_values where meter_id='" + InstRow["meter_id"] + "' and source_time_stamp <= '" + dtSource_Time.ToString("yyyy-MM-dd HH:mm:ss") + "' order by source_time_stamp desc", mssqlcnn);
                    DataSet ds = new DataSet();
                    da.Fill(ds);
                    if (ds.Tables.Count > 0 && ds.Tables[0].Rows.Count > 1)
                    {
                        string eventWord = InstRow["P04"].ToString();
                        string oldeventWord = ds.Tables[0].Rows[1]["P04"].ToString();
                        //R-Phase - Voltage missing for 3 phase meter- Occurrence or Restoration
                        if (CompareEventWord(0, eventWord, oldeventWord) == "false")
                        {
                            eventworddbupdate(dtSource_Time, eventWord.Substring(0, 1).ToString(), 0, serial_number, "R-Phase - Voltage missing for 3 phase meter- Occurrence or Restoration", dtServerTime);
                        }
                        //Y-Phase - Voltage missing
                        if (CompareEventWord(1, eventWord, oldeventWord) == "false")
                        {
                            eventworddbupdate(dtSource_Time, eventWord.Substring(1, 1).ToString(), 1, serial_number, "Y-Phase - Voltage missing", dtServerTime);
                        }
                        //B-Phase - Voltage missing
                        if (CompareEventWord(2, eventWord, oldeventWord) == "false")
                        {
                            eventworddbupdate(dtSource_Time, eventWord.Substring(2, 1).ToString(), 2, serial_number, "B-Phase - Voltage missing", dtServerTime);
                        }
                        //Over voltage in any phase
                        if (CompareEventWord(3, eventWord, oldeventWord) == "false")
                        {
                            eventworddbupdate(dtSource_Time, eventWord.Substring(3, 1).ToString(), 3, serial_number, "Over voltage in any phase", dtServerTime);
                        }
                        //Low voltage in any phase
                        if (CompareEventWord(4, eventWord, oldeventWord) == "false")
                        {
                            eventworddbupdate(dtSource_Time, eventWord.Substring(4, 1).ToString(), 4, serial_number, "Low voltage in any phase", dtServerTime);
                        }
                        //Voltage unbalance
                        if (CompareEventWord(5, eventWord, oldeventWord) == "false")
                        {
                            eventworddbupdate(dtSource_Time, eventWord.Substring(5, 1).ToString(), 5, serial_number, "Voltage unbalance", dtServerTime);
                        }
                        //R-Phase current reverse(Import type only)
                        if (CompareEventWord(6, eventWord, oldeventWord) == "false")
                        {
                            eventworddbupdate(dtSource_Time, eventWord.Substring(6, 1).ToString(), 6, serial_number, "R-Phase current reverse-Import type only", dtServerTime);
                        }
                        //Y-Phase current reverse(Import type only)
                        if (CompareEventWord(7, eventWord, oldeventWord) == "false")
                        {
                            eventworddbupdate(dtSource_Time, eventWord.Substring(7, 1).ToString(), 7, serial_number, "Y-Phase current reverse-Import type only", dtServerTime);
                        }
                        //B-Phase current reverse(Import type only)
                        if (CompareEventWord(8, eventWord, oldeventWord) == "false")
                        {
                            eventworddbupdate(dtSource_Time, eventWord.Substring(8, 1).ToString(), 8, serial_number, "B-Phase current reverse-Import type only", dtServerTime);
                        }
                        //Current unbalance
                        if (CompareEventWord(9, eventWord, oldeventWord) == "false")
                        {
                            eventworddbupdate(dtSource_Time, eventWord.Substring(9, 1).ToString(), 9, serial_number, "Current unbalance", dtServerTime);
                        }
                        //Current bypass/short
                        if (CompareEventWord(10, eventWord, oldeventWord) == "false")
                        {
                            eventworddbupdate(dtSource_Time, eventWord.Substring(10, 1).ToString(), 10, serial_number, "Current bypass/short", dtServerTime);
                        }
                        //Over current in any phase
                        if (CompareEventWord(11, eventWord, oldeventWord) == "false")
                        {
                            eventworddbupdate(dtSource_Time, eventWord.Substring(11, 1).ToString(), 11, serial_number, "Over current in any phase", dtServerTime);
                        }
                        //Very low PF
                        if (CompareEventWord(12, eventWord, oldeventWord) == "false")
                        {
                            eventworddbupdate(dtSource_Time, eventWord.Substring(12, 1).ToString(), 12, serial_number, "Very low PF", dtServerTime);
                        }
                        //Influence of permanent magnet or ac-dc electromagnet
                        if (CompareEventWord(81, eventWord, oldeventWord) == "false")
                        {
                            eventworddbupdate(dtSource_Time, eventWord.Substring(81, 1).ToString(), 81, serial_number, "Influence of permanent magnet or ac/dc electromagnet", dtServerTime);
                        }
                        //Neutral disturbance - HF,dc or alternate method
                        if (CompareEventWord(82, eventWord, oldeventWord) == "false")
                        {
                            eventworddbupdate(dtSource_Time, eventWord.Substring(82, 1).ToString(), 82, serial_number, "Neutral disturbance - HF-dc or alternate method", dtServerTime);
                        }
                        //Meter cover opening
                        if (CompareEventWord(83, eventWord, oldeventWord) == "false")
                        {
                            eventworddbupdate(dtSource_Time, eventWord.Substring(83, 1).ToString(), 83, serial_number, "Meter cover opening", dtServerTime);
                        }
                        //Meter load disconnected or Meter load connected
                        if (CompareEventWord(84, eventWord, oldeventWord) == "false")
                        {
                            eventworddbupdate(dtSource_Time, eventWord.Substring(84, 1).ToString(), 84, serial_number, "Meter load disconnected or Meter load connected", dtServerTime);
                        }
                        //Last Gasp - Occurrence
                        if (CompareEventWord(85, eventWord, oldeventWord) == "false")
                        {
                            eventworddbupdate(dtSource_Time, eventWord.Substring(85, 1).ToString(), 85, serial_number, "Last Gasp - Occurrence", dtServerTime);
                        }
                        //First Breath - Restoration
                        if (CompareEventWord(86, eventWord, oldeventWord) == "false")
                        {
                            eventworddbupdate(dtSource_Time, eventWord.Substring(86, 1).ToString(), 86, serial_number, "First Breath - Restoration", dtServerTime);
                        }
                        //Increment in billing counter - Manual or MRI reset
                        if (CompareEventWord(87, eventWord, oldeventWord) == "false")
                        {
                            eventworddbupdate(dtSource_Time, eventWord.Substring(87, 1).ToString(), 87, serial_number, "Increment in billing counter - Manual or MRI reset", dtServerTime);
                        }

                        UpdateSyncinfo(dtServerTime, "event_status_word_");
                        return "Success";
                    }
                    else
                    {
                        string eventWord = InstRow["P04"].ToString();
                        string oldeventWord = ds.Tables[0].Rows[1]["P04"].ToString();
                        //R-Phase - Voltage missing for 3 phase meter- Occurrence or Restoration

                        eventworddbupdate(dtSource_Time, eventWord.Substring(0, 1).ToString(), 0, serial_number, "R-Phase - Voltage missing for 3 phase meter- Occurrence or Restoration", dtServerTime);

                        //Y-Phase - Voltage missing
                        
                        eventworddbupdate(dtSource_Time, eventWord.Substring(1, 1).ToString(), 1, serial_number, "Y-Phase - Voltage missing", dtServerTime);

                        //B-Phase - Voltage missing
                        eventworddbupdate(dtSource_Time, eventWord.Substring(2, 1).ToString(), 2, serial_number, "B-Phase - Voltage missing", dtServerTime);

                        //Over voltage in any phase

                        eventworddbupdate(dtSource_Time, eventWord.Substring(3, 1).ToString(), 3, serial_number, "Over voltage in any phase", dtServerTime);

                        //Low voltage in any phase

                        eventworddbupdate(dtSource_Time, eventWord.Substring(4, 1).ToString(), 4, serial_number, "Low voltage in any phase", dtServerTime);

                        //Voltage unbalance

                        eventworddbupdate(dtSource_Time, eventWord.Substring(5, 1).ToString(), 5, serial_number, "Voltage unbalance", dtServerTime);

                        //R-Phase current reverse(Import type only)

                        eventworddbupdate(dtSource_Time, eventWord.Substring(6, 1).ToString(), 6, serial_number, "R-Phase current reverse-Import type only", dtServerTime);

                        //Y-Phase current reverse(Import type only)

                        eventworddbupdate(dtSource_Time, eventWord.Substring(7, 1).ToString(), 7, serial_number, "Y-Phase current reverse-Import type only", dtServerTime);

                        //B-Phase current reverse(Import type only)

                        eventworddbupdate(dtSource_Time, eventWord.Substring(8, 1).ToString(), 8, serial_number, "B-Phase current reverse-Import type only", dtServerTime);

                        //Current unbalance

                        eventworddbupdate(dtSource_Time, eventWord.Substring(9, 1).ToString(), 9, serial_number, "Current unbalance", dtServerTime);

                        //Current bypass/short

                        eventworddbupdate(dtSource_Time, eventWord.Substring(10, 1).ToString(), 10, serial_number, "Current bypass/short", dtServerTime);

                        //Over current in any phase

                        eventworddbupdate(dtSource_Time, eventWord.Substring(11, 1).ToString(), 11, serial_number, "Over current in any phase", dtServerTime);

                        //Very low PF

                        eventworddbupdate(dtSource_Time, eventWord.Substring(12, 1).ToString(), 12, serial_number, "Very low PF", dtServerTime);

                        //Influence of permanent magnet or ac-dc electromagnet

                        eventworddbupdate(dtSource_Time, eventWord.Substring(81, 1).ToString(), 81, serial_number, "Influence of permanent magnet or ac/dc electromagnet", dtServerTime);

                        //Neutral disturbance - HF,dc or alternate method

                        eventworddbupdate(dtSource_Time, eventWord.Substring(82, 1).ToString(), 82, serial_number, "Neutral disturbance - HF-dc or alternate method",dtServerTime);

                        //Meter cover opening

                        eventworddbupdate(dtSource_Time, eventWord.Substring(83, 1).ToString(), 83, serial_number, "Meter cover opening", dtServerTime);

                        //Meter load disconnected or Meter load connected

                        eventworddbupdate(dtSource_Time, eventWord.Substring(84, 1).ToString(), 84, serial_number, "Meter load disconnected or Meter load connected", dtServerTime);

                        //Last Gasp - Occurrence

                        eventworddbupdate(dtSource_Time, eventWord.Substring(85, 1).ToString(), 85, serial_number, "Last Gasp - Occurrence", dtServerTime);

                        //First Breath - Restoration

                        eventworddbupdate(dtSource_Time, eventWord.Substring(86, 1).ToString(), 86, serial_number, "First Breath - Restoration", dtServerTime);

                        //Increment in billing counter - Manual or MRI reset

                        eventworddbupdate(dtSource_Time, eventWord.Substring(87, 1).ToString(), 87, serial_number, "Increment in billing counter - Manual or MRI reset", dtServerTime);
                        
                        UpdateSyncinfo(dtServerTime, "event_status_word_");
                        
                        return "Success";
                    }                    
                }
                return "Fail";
            }
            catch (Exception ex)
            {
                Logs.Logging_Transaction message_log = new Logs.Logging_Transaction("events_occured", ex.Message);
                return ex.Message;
            }
            finally
            {
                mysqlcnn.Close();
            }
        }

        static void eventworddbupdate(DateTime dtSourceTime,string eventwordValue,int position,string serial_number,string event_name,DateTime dtServerTime)
        {
            try
            {
                string Event_Code = string.Empty;
                try
                {
                    SqlCommand cmd1 = new SqlCommand("select event_code from immediate_events where position_id='" + position + "' and occurence_or_reoccurence='" + eventwordValue + "'", mssqlcnn);
                    if (mssqlcnn.State == ConnectionState.Closed)
                    {
                        mssqlcnn.Open();
                    }
                    Event_Code = cmd1.ExecuteScalar().ToString();
                }
                catch (RowNotInTableException ex)
                {
                    string error = ex.Message;
                }


                MySqlCommand cmd = new MySqlCommand("INSERT INTO events_occured VALUES('" + serial_number + "','" + dtSourceTime.ToString("yyyy-MM-dd HH:mm:ss") + "','" + dtServerTime.ToString("yyyy-MM-dd HH:mm:ss") + "','" + Event_Code + "','" + event_name + "')", mysqlcnn);
                if (mysqlcnn.State == System.Data.ConnectionState.Closed)
                {
                    mysqlcnn.Open();
                }
                int k = cmd.ExecuteNonQuery();
                if (k > 0)
                {
                    string strInfo = "data inserted successfully";
                }                
            }
            catch (Exception ex)
            {
                Logs.Logging_Transaction message_log = new Logs.Logging_Transaction("events_occured", ex.Message);
            }            
        }

        static string CompareEventWord(int position,string streventword,string stroldeventword)
        {
            try
            {
                if (streventword.Substring(position, 1).ToString() == stroldeventword.Substring(position, 1).ToString())
                {
                    return "true";
                }
                else
                {
                    return "false";
                }
            }
            catch (Exception ex)
            {
                return ex.Message;
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
