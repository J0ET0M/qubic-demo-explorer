namespace QubicExplorer.Indexer.Configuration;

public class ClickHouseOptions
{
    public const string SectionName = "ClickHouse";

    public string Host { get; set; } = "localhost";
    public int Port { get; set; } = 8123;
    public string Database { get; set; } = "qubic";
    public string? Username { get; set; }
    public string? Password { get; set; }

    public string ConnectionString =>
        $"Host={Host};Port={Port};Database={Database}" +
        (string.IsNullOrEmpty(Username) ? "" : $";Username={Username}") +
        (string.IsNullOrEmpty(Password) ? "" : $";Password={Password}");

    /// <summary>
    /// Connection string without database — used for initial schema creation.
    /// </summary>
    public string ServerConnectionString =>
        $"Host={Host};Port={Port}" +
        (string.IsNullOrEmpty(Username) ? "" : $";Username={Username}") +
        (string.IsNullOrEmpty(Password) ? "" : $";Password={Password}");
}
