using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Container = Microsoft.Azure.Cosmos.Container;
//using PartitionKey = Microsoft.Azure.Cosmos.PartitionKey;
using Newtonsoft.Json;

namespace CosmosdUtls
{
    public class CosmosDbClient<TContext> : ICosmosDbClient<TContext> where TContext : ContainerContext
    {
        private readonly Container _container;
        private readonly int MAX_ITEM_COUNT;
        private readonly int Batch_Size;
        private readonly int ItemsSize = 1900000;
        public CosmosDbClient(TContext containterConext, IConfiguration configuration)
        {
            _container = containterConext.Container;
            MAX_ITEM_COUNT = 60;
            string size = configuration["cosmosParallelBatchSize"] ?? "1000";
            Batch_Size = int.Parse(size);
        }

        public async Task<bool> ParallelInsertLargeAsync<T>(IEnumerable<T> items, string partitionId)
        {
            bool final = false;

            List<bool> result = new List<bool>();
            List<IEnumerable<T>> list = ListExtension.Split(items, Batch_Size);

            foreach (var item in list)
            {
                var r = await ParallelInsertAsync(item, partitionId);
                result.Add(r);
                if (!r)
                {
                    Console.WriteLine($"ParallelInsertLargeAsync({item.Count()} item) failed");
                    break;
                }
            }

            final = result.All(c => c);

            return final;
        }

        private async Task<bool> ParallelInsertAsync<T>(IEnumerable<T> items, string partitionId)
        {
            bool final = false;
            List<IEnumerable<T>> list = ListExtension.Split(items, MAX_ITEM_COUNT);
            List<Task<bool>> tasks = new List<Task<bool>>();
            foreach (var item in list)
            {
                Task<bool> task = InsertBatchAsync(item, partitionId);
                tasks.Add(task);
            }
            
            try
            {
                var allTask = Task.WhenAll(tasks);
                bool[] results = await allTask;

                if (allTask.Status == TaskStatus.RanToCompletion)
                {
                    var goods = results.Where(c => c).ToList();
                    var bads = results.Where(c => !c).ToList();
                    if (bads.Any())
                    {
                        Console.WriteLine($"There are {bads.Count} InsertBatchAsync failed");
                    }
                    final = results.All(c => c);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ParallelInsertAsync failed, error = {0} ", e.Message);
            }

            return final;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="items">can work more than 100 items</param>
        /// <param name="partitionId"></param>
        /// <returns></returns>
        public async Task<bool> InsertLargeAsync<T>(IEnumerable<T> items, string partitionId)
        {
            List<IEnumerable<T>> list = ListExtension.Split(items, MAX_ITEM_COUNT);

            foreach (var item in list)
            {
                var suc = await InsertBatchAsync(item, partitionId);
                if (!suc)
                {
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="items">less than 100 items, total size less than 2MB, each item have the same partitionid</param>
        /// <param name="partitionId"></param>
        /// <returns></returns>
        public async Task<bool> InsertBatchAsync<T>(IEnumerable<T> items, string partitionId)
        {
            var datasize = JsonConvert.SerializeObject(items).Length;

            if (datasize <= ItemsSize)
            {
                return await InsertBatchTransactionAsync(items, partitionId);
            }

            bool final = false;
            try
            {
                var partitionKey = new PartitionKey(partitionId);
                var tasks = new List<Task<ItemResponse<T>>>();
                foreach (var item in items)
                {
                    var task = _container.CreateItemAsync<T>(item, partitionKey);
                    tasks.Add(task);
                }

                var allTask = Task.WhenAll(tasks);
                var results = await allTask;

                if (allTask.Status == TaskStatus.RanToCompletion)
                {
                    var allStatusCodes = results.Select(x => x.StatusCode).ToList();

                    var goods = allStatusCodes.Where(c => c.IsSuccessStatusCode()).ToList();
                    var bads = allStatusCodes.Where(c => !c.IsSuccessStatusCode()).ToList();

                    if (bads.Any())
                    {
                        Console.WriteLine($"There are {bads.Count} InsertBatchAsync failed");
                    }
                    else
                    {
                        Console.WriteLine($"There are {allStatusCodes.Count} InsertBatchAsync, using CreateItemAsync, success number {goods.Count}");
                        final = true;
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("InsertBatchAsync failed, error = {0} ", e.Message);
            }

            return final;
        }


        private async Task<bool> InsertBatchTransactionAsync<T>(IEnumerable<T> items, string partitionId)
        {
            PartitionKey partitionKey = new PartitionKey(partitionId);

            TransactionalBatch batch = _container.CreateTransactionalBatch(partitionKey);
            foreach (var item in items)
            {
                batch.CreateItem(item);
            }

            TransactionalBatchResponse response = await batch.ExecuteAsync();
            if (response.IsSuccessStatusCode)
            {
                //Console.WriteLine($"InsertBatchTransactionAsync success, code = {response.StatusCode}, item count={items.Count()}");
                return true;
            }
            else
            {
                var datasize = JsonConvert.SerializeObject(items).Length;
                Console.WriteLine($"InsertBatchTransactionAsync failed, code = {response.StatusCode}, dataSize={datasize}, item count={items.Count()}");
            }

            return false;
        }

        public async Task<T> CreateItemAsync<T>(T item, string partitionId = null)
        {
            if (item == null)
            {
                throw new ArgumentNullException("item");
            }
            PartitionKey? partitionKey = null;
            if (!string.IsNullOrWhiteSpace(partitionId))
            {
                partitionKey = new PartitionKey(partitionId);
            }
            try
            {
                var response = await _container.CreateItemAsync<T>(item, partitionKey).ConfigureAwait(false);
                return response.Resource;
            }
            catch (Exception ex)
            {

                throw;
            }

        }

        public async Task<T> ExecuteStoreProcedureAsync<T>(string procedureId, string partitionValue, dynamic[] parameters)
        {
            var response = await _container.Scripts.ExecuteStoredProcedureAsync<T>(procedureId, new PartitionKey(partitionValue), parameters).ConfigureAwait(false);
            if (response == null)
            {
                return default(T);
            }
            return response.Resource;
        }

        public async Task<T> GetItemAsync<T>(string partitionId, string id)
        {
            try
            {
                var response = await _container.ReadItemAsync<T>(id, new PartitionKey(partitionId));
                return response.Resource;
            }
            catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return default(T);
            }
        }

        public async Task<IEnumerable<T>> GetItemsAsync<T>(IQueryable<T> query)
        {
            List<T> list = new List<T>();
            int batchCount = 0;
            double requestCharge = 0;
            using (var feed = query.ToFeedIterator())
            {
                while (feed.HasMoreResults)
                {
                    var response = await feed.ReadNextAsync().ConfigureAwait(false);
                    requestCharge += response.RequestCharge;
                    list.AddRange(response);
                    batchCount++;
                }
            }
            return list;
        }

        public async Task<BaseResultSet<T>> GetItemsByPageAsync<T>(IQueryable<T> query, string continuationToken, int pageSize,
            string partitionId = null)
        {
            List<T> list = new List<T>();
            QueryRequestOptions options = new QueryRequestOptions();
            if (!string.IsNullOrWhiteSpace(partitionId))
            {
                options.PartitionKey = new PartitionKey(partitionId);
            }
            else
            {
                options.MaxConcurrency = -1;
            }

            int batchCount = 0;
            double requestCharge = 0;
            if (pageSize < 0)
            {
                pageSize = 0;
            }
            if (string.IsNullOrWhiteSpace(continuationToken))
            {
                continuationToken = null;
            }

            do
            {
                int recordToFetch = pageSize - list.Count;
                if (recordToFetch > MAX_ITEM_COUNT)
                {
                    recordToFetch = MAX_ITEM_COUNT;
                }
                options.MaxItemCount = recordToFetch > 0 ? recordToFetch : -1;
                options.MaxBufferedItemCount = options.MaxItemCount;
                options.MaxConcurrency = options.MaxBufferedItemCount;
                using (var feed = _container.GetItemQueryIterator<T>(query.ToQueryDefinition<T>(), continuationToken, options))
                {
                    do
                    {
                        var response = await feed.ReadNextAsync().ConfigureAwait(false);
                        requestCharge += response.RequestCharge;
                        continuationToken = response.ContinuationToken;
                        list.AddRange(response);

                        batchCount++;
                    }
                    while (feed.HasMoreResults && (options.MaxItemCount == -1 || Math.Abs(pageSize - list.Count) >= MAX_ITEM_COUNT));
                }

            }
            while (continuationToken != null && list.Count != pageSize);

            return new BaseResultSet<T>() { BaseTypeValues = list, PageContinuationToken = continuationToken };
        }


        public IQueryable<T> GetItemsQueryable<T>()
        {
            return _container.GetItemLinqQueryable<T>();
        }

        public async Task<T> UpdateItemAsync<T>(T item, string id)
        {
            if (item == null)
            {
                throw new ArgumentNullException(nameof(item));
            }
            var response = await _container.ReplaceItemAsync(item, id).ConfigureAwait(false);
            return response.Resource;
        }

        //public async Task<bool> DeleteAllItemsByPartitionAsync(string partitionId)
        //{
        //    ResponseMessage deleteResponse = await _container.DeleteAllItemsByPartitionKeyStreamAsync(new PartitionKey(partitionId));
        //    if (deleteResponse.IsSuccessStatusCode)
        //    {
        //        return true;
        //    }
        //    return false;
        //}
    }
}
