using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;

namespace CosmosdUtls
{
    public static class ListExtension
    {
        public static List<IEnumerable<T>> Split<T>(IEnumerable<T> items,int chunkSize)
        {
            List<IEnumerable<T>> list = new List<IEnumerable<T>>();

            var processed = 0;
            var hasNextBatch = true;

            while (hasNextBatch)
            {
                List<T> batch = items.Skip(processed).Take(chunkSize).ToList();
                if (batch.Count != 0)
                {
                    list.Add(batch);
                }

                processed += batch.Count;

                hasNextBatch = batch.Count == chunkSize;
            }

            return list;
        }
            
    }
}
