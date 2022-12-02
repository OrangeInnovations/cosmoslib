using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CosmosdUtls
{
    public interface ICosmosDbClient<TContext> where TContext: ContainerContext
    {
        Task<bool> ParallelInsertLargeAsync<T>(IEnumerable<T> items, string partitionId);

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="items">can work more than 100 items</param>
        /// <param name="partitionId"></param>
        /// <returns></returns>
        Task<bool> InsertLargeAsync<T>(IEnumerable<T> items, string partitionId);

        /// <summary>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="items">less than 100 items, total size less than 2MB, each item have the same partitionid</param>
        /// <param name="partitionId"></param>
        /// <returns></returns>
        Task<bool> InsertBatchAsync<T>(IEnumerable<T> items, string partitionId);
        IQueryable<T> GetItemsQueryable<T>();
        Task<IEnumerable<T>> GetItemsAsync<T>(IQueryable<T> query);
        Task<T> GetItemAsync<T>(string partitionId, string id);
        Task<T> CreateItemAsync<T>(T item,string partitionId=null);
        Task<T> UpdateItemAsync<T>(T item, string id);
        Task<BaseResultSet<T>> GetItemsByPageAsync<T>(IQueryable<T> query, string continuationToken, int pageSize, string partitionId = null);

        Task<T> ExecuteStoreProcedureAsync<T>(string procedureId, string partitionValue, dynamic[] parameters);
        //Task<bool> DeleteAllItemsByPartitionAsync(string partitionId);
    }
}
