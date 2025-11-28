namespace Tests.Ydb
{
    /// <summary>
    /// Replacement for ActiveIssue from the original LinqToDB tests.
    /// Does nothing, but allows us to keep the attributes in place.
    /// </summary>
    [AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = false)]
    internal sealed class ActiveIssueAttribute : Attribute
    {
        public string Url { get; }

        public ActiveIssueAttribute(string url)
        {
            Url = url;
        }
    }
}