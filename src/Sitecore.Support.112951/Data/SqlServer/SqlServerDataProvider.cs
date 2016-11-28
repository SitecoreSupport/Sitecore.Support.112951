namespace Sitecore.Support.Data.SqlServer
{
    using Sitecore.Eventing;

    public class SqlServerDataProvider : Sitecore.Data.SqlServer.SqlServerDataProvider
    {
        public SqlServerDataProvider(string connectionString) : base(connectionString)
        {
        }

        public override EventQueue GetEventQueue()
        {
            return new Sitecore.Support.Data.Eventing.SqlServerEventQueue(base.Api, base.Database);
        }
    }
}