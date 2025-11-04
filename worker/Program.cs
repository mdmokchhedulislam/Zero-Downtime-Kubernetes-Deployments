using System;
using System.Data.Common;
using System.Linq;
using System.Threading;
using Newtonsoft.Json;
using Npgsql;
using StackExchange.Redis;

namespace Worker
{
    public class Program
    {
        public static int Main(string[] args)
        {
            try
            {
                // === Read Environment Variables ===
                string pgHost = Environment.GetEnvironmentVariable("POSTGRES_HOST") ?? "localhost";
                string pgPort = Environment.GetEnvironmentVariable("POSTGRES_PORT") ?? "5432";
                string pgDb = Environment.GetEnvironmentVariable("POSTGRES_DB") ?? "persondb";
                string pgUser = Environment.GetEnvironmentVariable("POSTGRES_USER") ?? "postgres";
                string pgPassword = Environment.GetEnvironmentVariable("POSTGRES_PASSWORD") ?? "password123";

                string redisHost = Environment.GetEnvironmentVariable("REDIS_HOST") ?? "localhost";
                string redisPort = Environment.GetEnvironmentVariable("REDIS_PORT") ?? "6379";

                int sleepMs = int.Parse(Environment.GetEnvironmentVariable("SLEEP_MS") ?? "100");

                // === PostgreSQL Connection ===
                string connStr = $"Host={pgHost};Port={pgPort};Username={pgUser};Password={pgPassword};Database={pgDb};";
                var pgsql = OpenDbConnection(connStr);

                // === Redis Connection ===
                var redisConn = OpenRedisConnection($"{redisHost}:{redisPort}");
                var redis = redisConn.GetDatabase();

                // Keep-alive workaround
                var keepAliveCommand = pgsql.CreateCommand();
                keepAliveCommand.CommandText = "SELECT 1";

                var definition = new { vote = "", voter_id = "" };

                while (true)
                {
                    Thread.Sleep(sleepMs);

                    if (redisConn == null || !redisConn.IsConnected)
                    {
                        Console.WriteLine("Reconnecting Redis...");
                        redisConn = OpenRedisConnection($"{redisHost}:{redisPort}");
                        redis = redisConn.GetDatabase();
                    }

                    string json = redis.ListLeftPopAsync("votes").Result;
                    if (json != null)
                    {
                        var vote = JsonConvert.DeserializeAnonymousType(json, definition);
                        Console.WriteLine($"Processing vote for '{vote.vote}' by '{vote.voter_id}'");

                        if (pgsql.State != System.Data.ConnectionState.Open)
                        {
                            Console.WriteLine("Reconnecting DB...");
                            pgsql = OpenDbConnection(connStr);
                        }

                        UpdateVote(pgsql, vote.voter_id.ToString(), vote.vote);
                    }
                    else
                    {
                        keepAliveCommand.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.ToString());
                return 1;
            }
        }

        private static NpgsqlConnection OpenDbConnection(string connectionString)
        {
            NpgsqlConnection connection;

            while (true)
            {
                try
                {
                    connection = new NpgsqlConnection(connectionString);
                    connection.Open();
                    break;
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine("Waiting for DB: " + ex.Message);
                    Thread.Sleep(1000);
                }
            }

            Console.Error.WriteLine("Connected to DB");

            using var command = connection.CreateCommand();
            command.CommandText = @"CREATE TABLE IF NOT EXISTS votes (
                                        id VARCHAR(255) NOT NULL UNIQUE,
                                        vote VARCHAR(255) NOT NULL
                                    );";
            command.ExecuteNonQuery();

            return connection;
        }

        private static ConnectionMultiplexer OpenRedisConnection(string hostAndPort)
        {
            while (true)
            {
                try
                {
                    Console.Error.WriteLine("Connecting to Redis at " + hostAndPort);
                    return ConnectionMultiplexer.Connect(hostAndPort);
                }
                catch (RedisConnectionException ex)
                {
                    Console.Error.WriteLine("Waiting for Redis: " + ex.Message);
                    Thread.Sleep(1000);
                }
            }
        }

        private static void UpdateVote(NpgsqlConnection connection, string voterId, string vote)
        {
            using var command = connection.CreateCommand();
            try
            {
                command.CommandText = "INSERT INTO votes (id, vote) VALUES (@id, @vote)";
                command.Parameters.AddWithValue("@id", voterId);
                command.Parameters.AddWithValue("@vote", vote);
                command.ExecuteNonQuery();
            }
            catch (DbException)
            {
                command.CommandText = "UPDATE votes SET vote = @vote WHERE id = @id";
                command.Parameters.AddWithValue("@id", voterId);
                command.Parameters.AddWithValue("@vote", vote);
                command.ExecuteNonQuery();
            }
        }
    }
}
