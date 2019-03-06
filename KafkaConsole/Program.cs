using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KafkaLib;

namespace KafkaConsole
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var dir = AppDomain.CurrentDomain.BaseDirectory;
            var myProducer = new MyProducer("10.0.0.6:9092", "MyTopic", $"{dir}\\kafka.log", $"{dir}\\app.log");
            int i = 1;
            Console.WriteLine("Sending message, please check out the log file. Press CTRL+C to exit.");
            while (true)
            {
                await myProducer.SendMessage(i.ToString());

                i++;
                await Task.Delay(5000);
;            }
        }
    }
}
