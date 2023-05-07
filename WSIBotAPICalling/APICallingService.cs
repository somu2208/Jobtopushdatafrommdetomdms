using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace WSIBotAPICalling
{
    partial class APICallingService : ServiceBase
    {
  
        Thread thWakeUpWinApp;
        public APICallingService()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            // TODO: Add code here to start your service.
            thWakeUpWinApp = new Thread(new ThreadStart(WakeupAppThread));
            thWakeUpWinApp.Start();
        }

        protected override void OnStop()
        {
            // TODO: Add code here to perform any tear-down necessary to stop your service.
            thWakeUpWinApp.Abort();
        }

        private void WakeupAppThread()
        {
            int sleepTime_seconds = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["SleepTime"]);
            while (true)
            {
                //MDMS DB ables insert

                SMDataMSSQLtoMySQLWJ.MDMS.Move_Instantaneous MI = new SMDataMSSQLtoMySQLWJ.MDMS.Move_Instantaneous();
                SMDataMSSQLtoMySQLWJ.MDMS.Move_Blockload MBLL = new SMDataMSSQLtoMySQLWJ.MDMS.Move_Blockload();
                SMDataMSSQLtoMySQLWJ.MDMS.Move_DailyLoad MDL = new SMDataMSSQLtoMySQLWJ.MDMS.Move_DailyLoad();
                SMDataMSSQLtoMySQLWJ.MDMS.Move_BillingLoad MBILL = new SMDataMSSQLtoMySQLWJ.MDMS.Move_BillingLoad();
                SMDataMSSQLtoMySQLWJ.MDMS.Move_DisconnectControlEventLog MDCEL = new SMDataMSSQLtoMySQLWJ.MDMS.Move_DisconnectControlEventLog();
                SMDataMSSQLtoMySQLWJ.MDMS.Move_CurrentEventLog MCEL = new SMDataMSSQLtoMySQLWJ.MDMS.Move_CurrentEventLog();
                SMDataMSSQLtoMySQLWJ.MDMS.Move_VoltageEventLog MVEL = new SMDataMSSQLtoMySQLWJ.MDMS.Move_VoltageEventLog();
                SMDataMSSQLtoMySQLWJ.MDMS.Move_NonrolloverEventLog MNEL = new SMDataMSSQLtoMySQLWJ.MDMS.Move_NonrolloverEventLog();
                SMDataMSSQLtoMySQLWJ.MDMS.Move_OtherEventLog MOE = new SMDataMSSQLtoMySQLWJ.MDMS.Move_OtherEventLog();
                SMDataMSSQLtoMySQLWJ.MDMS.Move_PowerEvent MPEL = new SMDataMSSQLtoMySQLWJ.MDMS.Move_PowerEvent();
                SMDataMSSQLtoMySQLWJ.MDMS.Move_TransactionEvent MTEL = new SMDataMSSQLtoMySQLWJ.MDMS.Move_TransactionEvent();
                SMDataMSSQLtoMySQLWJ.MDMS.Move_Namespace MNSP = new SMDataMSSQLtoMySQLWJ.MDMS.Move_Namespace();
                SMDataMSSQLtoMySQLWJ.MDMS.Move_Programmable_Parameters MPP = new SMDataMSSQLtoMySQLWJ.MDMS.Move_Programmable_Parameters();
                SMDataMSSQLtoMySQLWJ.MDMS.Move_EventStatusWordCount MDASEWS = new SMDataMSSQLtoMySQLWJ.MDMS.Move_EventStatusWordCount();

                System.Threading.Thread.Sleep(1000* sleepTime_seconds);
            }
        }
    }
}
