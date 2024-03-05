using DbSyncKit.DB.Enum;
using DbSyncKit.DB.Interface;
using Microsoft.Data.SqlClient;
using System.Data;

namespace DbSyncKit.MSSQL
{
    /// <summary>
    /// Represents a connection to a Microsoft SQL Server database.
    /// </summary>
    public class Connection : IDatabase
    {
        #region Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="Connection"/> class.
        /// </summary>
        public Connection()
        {

        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Connection"/> class with the specified parameters.
        /// </summary>
        /// <param name="serverAddress">The address of the server.</param>
        /// <param name="useSQLAuthentication">Indicates whether to use SQL authentication.</param>
        /// <param name="databaseName">The name of the database.</param>
        /// <param name="sqlUsername">The username for SQL authentication.</param>
        /// <param name="sqlPassword">The password for SQL authentication.</param>
        public Connection(string serverAddress, bool useSQLAuthentication, string? databaseName = null, string? sqlUsername = null, string? sqlPassword = null)
        {
            Server = serverAddress;
            UseSQLAuthentication = useSQLAuthentication;

            if (databaseName != null)
                DatabaseName = databaseName;

            if (useSQLAuthentication)
            {
                SQLUsername = sqlUsername;
                SQLPassword = sqlPassword;
            }
        }

        #endregion


        #region Properties
        /// <summary>
        /// Gets or sets the address of the server.
        /// </summary>
        private string Server { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether to use SQL authentication.
        /// </summary>
        private bool UseSQLAuthentication { get; set; }

        /// <summary>
        /// Gets or sets the username for SQL authentication.
        /// </summary>
        private string? SQLUsername { get; set; }

        /// <summary>
        /// Gets or sets the password for SQL authentication.
        /// </summary>
        private string? SQLPassword { get; set; }

        /// <summary>
        /// Gets or sets the name of the database.
        /// </summary>
        private string? DatabaseName { get; set; }

        /// <summary>
        /// Gets the database provider type, which is MSSQL for this class.
        /// </summary>
        public DatabaseProvider Provider => DatabaseProvider.MSSQL;

        #endregion

        #region Public Methods

        /// <summary>
        /// Tests the database connection.
        /// </summary>
        /// <returns>True if the connection is successful; otherwise, false.</returns>
        public bool TestConnection()
        {
            string connectionString = string.Empty;
            try
            {
                connectionString = GetConnectionString();
                using (SqlConnection sqlConnection = new(connectionString))
                {
                    try
                    {
                        sqlConnection.Open();
                        sqlConnection.Close();
                        return true;
                    }
                    catch (SqlException)
                    {
                        throw;
                    }
                }
            }
            catch (Exception)
            {
                throw;
            }
        }

        /// <summary>
        /// Gets the connection string based on the provided parameters.
        /// </summary>
        /// <returns>The connection string.</returns>
        public string GetConnectionString()
        {
            SqlConnectionStringBuilder sqlConnectionStringBuilder = new();
            sqlConnectionStringBuilder.DataSource = Server;
            sqlConnectionStringBuilder.IntegratedSecurity = !UseSQLAuthentication;

            if (DatabaseName != null)
                sqlConnectionStringBuilder.InitialCatalog = DatabaseName;

            if (UseSQLAuthentication)
            {
                sqlConnectionStringBuilder.UserID = SQLUsername;
                sqlConnectionStringBuilder.Password = SQLPassword;
            }

            sqlConnectionStringBuilder.TrustServerCertificate = true;

            return sqlConnectionStringBuilder.ConnectionString;
        }

        #endregion

        #region Static Method

        /// <summary>
        /// Executes a SQL query and returns the result as a DataSet.
        /// </summary>
        /// <param name="query">The SQL query string.</param>
        /// <param name="tableName">The name to be assigned to the result table in the DataSet.</param>
        /// <returns>A DataSet containing the result of the query.</returns>
        public DataSet ExecuteQuery(string query, string tableName)
        {
            using (SqlConnection sqlConnection = new(this.GetConnectionString()))
            {
                try
                {
                    sqlConnection.Open();

                    using (SqlDataAdapter sqlDataAdapter = new(query, sqlConnection))
                    using (DataSet schema = new())
                    {
                        sqlDataAdapter.Fill(schema, tableName);

                        return schema;
                    }
                }
                catch (Exception ex)
                {
                    // Handle the exception, log it, or rethrow a more specific exception.
                    throw new Exception("Error executing query: " + ex.Message);
                }
            }
        }

        #endregion
    }
}
