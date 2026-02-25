using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    public class RoundRobinClusterClient : ClusterClientBase
    {
        public RoundRobinClusterClient(string[] replicaAddresses) : base(replicaAddresses)
        {
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var start = DateTime.Now;
            var i = 0;
            
            foreach (var replicaAddress in ReplicaAddresses)
            {
                var elapsed = DateTime.Now - start;
                var timeLeft = timeout - elapsed;
                var perReplicaTimeout = TimeSpan.FromMilliseconds(timeLeft.TotalMilliseconds / (ReplicaAddresses.Length - i));
                i++;
                var request = CreateRequest(replicaAddress + "?query=" + query);
                var task = ProcessRequestAsync(request);
                var delayTask = Task.Delay(perReplicaTimeout);
                var completedTask = await Task.WhenAny(task, delayTask);
                if (completedTask == task)
                {
                    try
                    {
                        return await task;
                    }
                    catch (Exception)
                    {
                    }
                }
            }
            throw new TimeoutException($"Request {query} timed out");
        }

        protected override ILog Log => LogManager.GetLogger(typeof(RoundRobinClusterClient));
    }
}
