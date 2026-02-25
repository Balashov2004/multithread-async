using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    public class SmartClusterClient : ClusterClientBase
    {
        public SmartClusterClient(string[] replicaAddresses) : base(replicaAddresses)
        {
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var tasks = new List<Task<string>>();
            var perReplicaTimeout = TimeSpan.FromMilliseconds(timeout.TotalMilliseconds / ReplicaAddresses.Length);
            var sw = Stopwatch.StartNew();

            for (int i = 0; i < ReplicaAddresses.Length; i++)
            {
                var request = CreateRequest(ReplicaAddresses[i] + "?query=" + query);
                var currentTask = ProcessRequestAsync(request);
                tasks.Add(currentTask);
                
                while (true)
                {
                    var timePassed = sw.Elapsed;
                    var timeToNextLaunch = perReplicaTimeout * (i + 1) - timePassed;
                    
                    if (i == ReplicaAddresses.Length - 1)
                        timeToNextLaunch = timeout - timePassed;

                    var delayTask = Task.Delay(timeToNextLaunch);
                    var completedTask = await Task.WhenAny(Task.WhenAny(tasks), delayTask);

                    if (completedTask != delayTask)
                    {
                        var finishedTask = await (Task<Task<string>>)completedTask;
                        try
                        {
                            return await finishedTask;
                        }
                        catch (Exception)
                        {
                            tasks.Remove(finishedTask);
                            if (tasks.Count == 0 || i < ReplicaAddresses.Length - 1)
                            {
                                goto next;
                            }
                        }
                    }
                    else
                    {
                        break;
                    }
                }

                next: ;
            }

            throw new TimeoutException();
        }

        protected override ILog Log => LogManager.GetLogger(typeof(SmartClusterClient));
    }
}