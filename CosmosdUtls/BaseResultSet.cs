namespace CosmosdUtls
{
    public class BaseResultSet<TEntity>
    {
        public IEnumerable<TEntity> BaseTypeValues { get; set; }
        public int ResultCount { get; set; }
        public string PageContinuationToken { get; set; }
    }
}