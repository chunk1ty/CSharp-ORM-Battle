namespace QueryOptimizations
{
    public static class Settings
    {
        // public const string ConnectionString = @"Server=.\;Database=CatsDemoDb;Trusted_Connection=True;MultipleActiveResultSets=True;";
        public const string ConnectionString = @"
   Server=127.0.0.1,1433;
   Database=CatsDemoDb;
   User Id=SA;
   Password=yourStrong(!)Password
";
    }
}
