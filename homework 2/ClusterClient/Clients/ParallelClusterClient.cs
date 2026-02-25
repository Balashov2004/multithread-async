using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    public class ParallelClusterClient : ClusterClientBase
    {
        public ParallelClusterClient(string[] replicaAddresses) : base(replicaAddresses)
        {
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var tasks = ReplicaAddresses
                .Select(uri => CreateRequest(uri + "?query=" + query))
                .Select(ProcessRequestAsync)
                .ToList();
            
            var delayTask = Task.Delay(timeout);
            while (tasks.Count != 0)
            {
                var processTask = await Task.WhenAny(Task.WhenAny(tasks), delayTask);
                await Task.WhenAny(processTask, delayTask);
                if (delayTask.IsCompleted)
                {
                    throw new TimeoutException();
                }
                var completedTask = await (Task<Task<string>>)processTask;
                tasks.Remove(completedTask);
                
                try
                {
                    return await completedTask;
                }
                catch (Exception)
                {
                    Log.Error("Task failed");
                }
            }
            return null;
        }

        protected override ILog Log => LogManager.GetLogger(typeof(ParallelClusterClient));
    }
}
