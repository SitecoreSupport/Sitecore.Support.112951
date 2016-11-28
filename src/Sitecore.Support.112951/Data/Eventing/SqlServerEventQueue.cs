namespace Sitecore.Support.Data.Eventing
{
    using System;
    using System.Collections.Generic;
    using Sitecore.Data;
    using Sitecore.Data.DataProviders.Sql;
    using Sitecore.Diagnostics;

    public class SqlServerEventQueue : Sitecore.Data.Eventing.SqlServerEventQueue
    {
        public SqlServerEventQueue(SqlDataApi api) : base(api)
        {
        }

        public SqlServerEventQueue(SqlDataApi api, Database database) : base(api, database)
        {
        }

        /// <summary>
        /// Adds the criteria.
        /// </summary>
        /// <param name="statement">The statement.</param>
        /// <param name="query">The query.</param>
        protected override void AddCriteria(SqlStatement statement, Sitecore.Eventing.EventQueueQuery query)
        {
            Assert.ArgumentNotNull(statement, "statement");
            Assert.ArgumentNotNull(query, "query");

            string where = string.Empty;
            string eventTypesWhere = string.Empty;
            var criteria = new List<string>();
            var eventTypeCriteria = new List<string>();

            if (query.SourceInstanceName != null)
            {
                criteria.Add("{0}InstanceName{1} = {2}sourceInstance{3}");
                statement.AddParameter("sourceInstance", query.SourceInstanceName);
            }

            if (query.TargetInstanceName != null)
            {
                criteria.Add(
                  "({0}InstanceName{1} <> {2}targetInstance{3} AND {0}RaiseGlobally{1} = 1 OR {0}InstanceName{1} = {2}targetInstance{3} AND {0}RaiseLocally{1} = 1)");
                statement.AddParameter("targetInstance", query.TargetInstanceName);
            }

            if (query.FromUtcDate != null)
            {
                criteria.Add("{0}Created{1} >= {2}fromUtcDate{3}");
                statement.AddParameter("fromUtcDate", query.FromUtcDate);
            }

            if (query.ToUtcDate != null)
            {
                criteria.Add("{0}Created{1} <= {2}toUtcDate{3}");
                statement.AddParameter("toUtcDate", query.ToUtcDate);
            }

            if (query.FromTimestamp != null)
            {
                criteria.Add("{0}Stamp{1} >= CAST({2}fromTimestamp{3} AS TIMESTAMP)");
                statement.AddParameter("fromTimestamp", query.FromTimestamp);
            }

            if (query.ToTimestamp != null)
            {
                criteria.Add("{0}Stamp{1} <= CAST({2}toTimestamp{3} AS TIMESTAMP)");
                statement.AddParameter("toTimestamp", query.ToTimestamp);
            }

            if (query.UserName != null)
            {
                criteria.Add("{0}UserName{1} = {2}userName{3}");
                statement.AddParameter("userName", query.UserName);
            }

            if (query.EventTypes != null)
            {
                int i = 0;
                foreach (Type eventType in query.EventTypes)
                {
                    string parameterName = string.Concat("eventType", i);

                    eventTypeCriteria.Add(string.Concat("{0}EventType{1} = {2}", parameterName, "{3}"));
                    statement.AddParameter(parameterName, eventType.AssemblyQualifiedName);
                    i++;
                }
            }

            if (query.InstanceType != null)
            {
                criteria.Add("{0}InstanceType{1} = {2}instanceType{3}");
                statement.AddParameter("instanceType", query.InstanceType.AssemblyQualifiedName);
            }

            foreach (string criterion in criteria)
            {
                if (where.Length > 0)
                {
                    where += " AND ";
                }

                where += criterion;
            }

            eventTypesWhere = base.GetEventTypeConditions(eventTypeCriteria);

            if (where.Length == 0 && eventTypesWhere.Length == 0)
            {
                return;
            }

            if (where.Length != 0 && eventTypesWhere.Length != 0)
            {
                statement.Where = string.Concat("WHERE ", where, " AND ", string.Format("({0})", eventTypesWhere)); //the fix is to group all event type conditions using parentheses
            }
            else
            {
                statement.Where = string.Concat("WHERE ", where, eventTypesWhere);
            }
        }
    }
}