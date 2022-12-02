using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Fluent;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using Polly;

namespace CosmosdUtls
{
    public static class CosmosDbHelper
    {
        public static CosmosClient CreateCosmosClient(string accountEndPoint, string authKey)
        {
            var builder = new CosmosClientBuilder(accountEndPoint, authKey)
                .WithRequestTimeout(new TimeSpan(0, 0, 300))
                .WithThrottlingRetryOptions(new TimeSpan(0, 0, 30), 20)
                .AddCustomHandlers(new ThrottlingHandler());

            return builder.Build();
        }

        private class ThrottlingHandler: RequestHandler
        {
            public override Task<ResponseMessage> SendAsync(
             RequestMessage request,
             CancellationToken cancellationToken)
            {
                var jitter = new Random();
                return Policy
                    .HandleResult<ResponseMessage>(r => (int)r.StatusCode == 429)
                    .WaitAndRetryAsync(5,
                        retryAttemp => TimeSpan.FromSeconds(Math.Pow(1.5, retryAttemp)) +
                                        TimeSpan.FromMilliseconds(jitter.Next(0, 100))
                    )
                    //.RetryAsync(3)
                    .ExecuteAsync(() => base.SendAsync(request, cancellationToken));
            }
        }
    }
}
