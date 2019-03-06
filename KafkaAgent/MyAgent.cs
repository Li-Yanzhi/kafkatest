using System;
using System.Collections.Concurrent;
using System.IO;
using System.Text;
using Microsoft.Exchange.Data.Transport;
using Microsoft.Exchange.Data.Transport.Routing;
using System.Threading.Tasks;
using KafkaLib;

namespace KafkaAgent
{
    public class MyAgent : RoutingAgent
    {
        private static readonly object Locker = new object();
        private static int i = 1;
        private static readonly ConcurrentQueue<string> Queue = new ConcurrentQueue<string>();
        const string Dir = @"C:\cmb\test\Diagnostic\KafkaAgent";

        static MyAgent()
        {
            StartDeliver();
        }

        public MyAgent(SmtpServer server)
        {
            OnCategorizedMessage += Logger_OnCategorizedMessage;
        }

        private void Logger_OnCategorizedMessage(CategorizedMessageEventSource source, QueuedMessageEventArgs e)
        {
            WriteLog($"{DateTime.Now}\tProcess\t{e.MailItem.Message.Subject}");

            Queue.Enqueue(i + "\t" + e.MailItem.Message.Subject);
            i++;
        }

        private static void StartDeliver()
        {
            WriteLog("Start Deliver");
            Task.Factory.StartNew(async () =>
            {
                while (true)
                {
                    if (Queue.TryDequeue(out var message))
                    {

                        var myProducer = new MyProducer("10.0.0.6:9092", "MyTopic", $"{Dir}\\kafka.log", $"{Dir}\\app.log");

                        var res = await myProducer.SendMessage(message);
                        await Task.Delay(1000);
                    }
                    else
                    {
                        await Task.Delay(5000);
                    }
                }
            }, TaskCreationOptions.LongRunning);
        }

        private static void WriteLog(string log)
        {
            lock (Locker)
            {
                using (var file = new FileStream($"{Dir}\\agent.log", FileMode.Append, FileAccess.Write, FileShare.Read))
                using (var writer = new StreamWriter(file, Encoding.Unicode))
                {
                    writer.WriteLine(log);
                }
            }
        }
    }
}