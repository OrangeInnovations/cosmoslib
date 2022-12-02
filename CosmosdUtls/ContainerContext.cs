using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;
using System.Text;

namespace CosmosdUtls
{
    public abstract class ContainerContext
    {
        private readonly Container _container;
        public ContainerContext(CosmosClient cosmosClient,string databaseId, string containerId)
        {
            _container= cosmosClient.GetContainer(databaseId, containerId);
        }
        public Container Container=>_container;
    }
}
