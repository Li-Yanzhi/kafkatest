using KafkaLib;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace KafkaService
{
    public partial class Service1 : ServiceBase
    {
        public Service1()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            Task.Factory.StartNew(async () =>
            {
                var dir = AppDomain.CurrentDomain.BaseDirectory;
                var myProducer = new MyProducer("10.0.0.6:9092", "MyTopic", $"{dir}\\kafka.log", $"{dir}\\app.log");
                int i = 1;
                while (true)
                {
                    await myProducer.SendMessage(i.ToString());

                    i++;
                    await Task.Delay(5000);
                }
            });
        }

        protected override void OnStop()
        {
        }
    }
}
