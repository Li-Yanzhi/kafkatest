using Microsoft.Exchange.Data.Transport;
using Microsoft.Exchange.Data.Transport.Routing;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaAgent
{
    public class MailAgentFactory : RoutingAgentFactory
    {
        public MailAgentFactory()
        {
        }

        public override RoutingAgent CreateAgent(SmtpServer server)
        {
            return new MyAgent(server);
        }
    }
}
