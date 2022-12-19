using System;
using MongoDB.Bson;
using MongoDB.Driver;
using Npgsql;
using StackExchange.Redis;

namespace ws
{
    class Program
    {
        static void Main(string[] args)
        {
            RedisCreator dbRedis = new RedisCreator();
            IDatabase redisDb = dbRedis.CreateDatabase();
            redisDb.Create(2,"test");
            Console.WriteLine(redisDb.Read(1));
            
            PostgresCreator dbPostgre = new PostgresCreator();
            IDatabase postgresDB = dbPostgre.CreateDatabase();
            postgresDB.Create(2,"hodnota");
            Console.WriteLine(postgresDB.Read(1));
            
            MongoCreator mongoCreator = new MongoCreator();
            IDatabase mongoDb = mongoCreator.CreateDatabase();
            mongoDb.Create(2,"hodnota");
            mongoDb.Delete(1);
            Console.WriteLine(mongoDb.Read(1));
        }
    }

public interface IDatabase
{

    // Create (přidání nového záznamu do databáze)
    void Create(int id, string value);

    // Read (čtení záznamů z databáze)
    string Read(int id);

    // Update (aktualizace existujícího záznamu v databázi)
    void Update(int id, string value);

    // Delete (odstranění záznamu z databáze)
    void Delete(int id);
}

public class Postgres : IDatabase
{
    private NpgsqlConnection conn;

    public Postgres(string connString)
    {
        this.conn = new NpgsqlConnection(connString);
    }
    
    public void Create(int id, string value)
    {
        // Implementace přidání nového záznamu do Postgres databáze
        conn.Open();
        using (var cmd = new NpgsqlCommand("INSERT INTO zapocet (id,value) VALUES (@val1, @val2)", conn))
        {
            cmd.Parameters.AddWithValue("val1", id);
            cmd.Parameters.AddWithValue("val2", value);
            int nRows = cmd.ExecuteNonQuery();
            Console.WriteLine("Počet vložených řádku do db=",nRows);
        }
        
    }

    public string Read(int id)
    {
        // Implementace čtení záznamu z Postgres databáze
        conn.Open();
        using (var cmd = new NpgsqlCommand("SELECT value FROM zapocet WHERE id = " + id, conn))
        {
            var reader = cmd.ExecuteReader();
            if (reader.HasRows)
            {
                reader.Read();
                var value = reader.GetValue(0).ToString();
                return value;
            }
            else
            {
                return null;
            }   
        }
    }

    public void Update(int id, string value)
    {
        // Implementace aktualizace záznamu v Postgres databázi
        using (var cmd = new NpgsqlCommand("UPDATE zapocet SET value = @value WHERE id = @id",conn))
        {
            cmd.Parameters.AddWithValue("value", value);
            cmd.Parameters.AddWithValue("id", id);
            int nRows = cmd.ExecuteNonQuery();
            Console.WriteLine(nRows);
        }
        
    }

    public void Delete(int id)
    {
        // Implementace odstranění záznamu z Postgres databáze
        using (var cmd = new NpgsqlCommand("DELETE FROM zapocet WHERE id = @id",conn))
        {
            cmd.Parameters.AddWithValue("id", id);
            int nRows= cmd.ExecuteNonQuery();
            Console.WriteLine(nRows);
        }
        
    }

}

public class Redis : IDatabase
{
    private ConnectionMultiplexer conn;
    public Redis(string connString)
    {
        this.conn = ConnectionMultiplexer.Connect(connString);
    }

    public void Create(int id, string value)
    {
        var db = conn.GetDatabase();
        db.StringSet(id.ToString(), value);
    }

    public string Read(int id)
    {
        var db = conn.GetDatabase();
        return db.StringGet(id.ToString());
    }

    public void Update(int id, string value)
    {
        var db = conn.GetDatabase();
        db.StringSet(id.ToString(), value);
    }

    public void Delete(int id)
    {
        var db = conn.GetDatabase();
        db.KeyDelete(id.ToString());
    }
}

public class MongoDB : IDatabase
{
    private IMongoDatabase conn;
    public MongoDB(string connString)
    {
        var client = new MongoClient(connString);
        this.conn = client.GetDatabase("zapocet");
    }

    public void Create(int id, string value)
    {
        var collection = conn.GetCollection<BsonDocument>("kolekce");

        var doc = new BsonDocument
        {
            { "id", id },
            { "value", value }
        };
        try
        {
            collection.InsertOne(doc);
        }
        catch (MongoWriteException e)
        {
            Console.WriteLine(e.Message);            
        }
        
    }

    public string Read(int id)
    {
        var collection = conn.GetCollection<BsonDocument>("kolekce");
        var filter = Builders<BsonDocument>.Filter.Eq("id", id);
        var doc = collection.Find(filter).FirstOrDefault();
        if (doc != null)
        {
            return doc["value"].AsString;
        }
        else
        {
            return null;
        }
    }

    public void Update(int id, string value)
    {
        var collection = conn.GetCollection<BsonDocument>("kolekce");
        var filter = Builders<BsonDocument>.Filter.Eq("id", id);
        var update = Builders<BsonDocument>.Update.Set("value", value);
        collection.UpdateOne(filter, update);
    }

    public void Delete(int id)
    {
        var collection = conn.GetCollection<BsonDocument>("kolekce");
        var filter = Builders<BsonDocument>.Filter.Eq("id", id);
        collection.DeleteOne(filter);
    }
}


public abstract class DatabaseCreator
{
    protected IDatabase db;

    public DatabaseCreator()
    {
        var config = File.ReadAllText("db.config");
        switch (config)
        {
            case "postgres":
                PostgresCreator postgres = new PostgresCreator();
                db = postgres.CreateDatabase();
                break;
            case "redis":
                RedisCreator redisCreator = new RedisCreator();
                db = redisCreator.CreateDatabase();
                break;
            case "mongodb":
                MongoCreator mongoCreator = new MongoCreator();
                db = mongoCreator.CreateDatabase();
                break;
            default:
                db = null;
                break;
        }
    }

    public virtual IDatabase CreateDatabase()
    {
        return db;
    }
}

public class PostgresCreator : DatabaseCreator
{
    public override IDatabase CreateDatabase()
    {
        db = new Postgres("Server=localhost;Username=postgres;Database=postgres;Port=5432;Password=password;SSLMode=Prefer");
        return db;
    }
}

public class RedisCreator : DatabaseCreator
{
    public override IDatabase CreateDatabase()
    {
        db = new Redis("localhost,connectTimeout=5000");
        return db;
    }
}

public class MongoCreator : DatabaseCreator
{
    public override IDatabase CreateDatabase()
    {
        db = new MongoDB("mongodb://root:toor@localhost:27017");
        return db;
    }
}


}