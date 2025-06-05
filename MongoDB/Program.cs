using System;
using MongoDB.Driver;
using MongoDB.Bson;

namespace MongoDB
{
    public class Program
    {
        static void Main(string[] args)
        {
            MongoClient client = new MongoClient("mongodb://localhost:27017");

            IMongoDatabase DataBase = client.GetDatabase("weather");
            IMongoCollection<BsonDocument> weatherCollection = DataBase.GetCollection<BsonDocument>("weather");

            /*var pipeline = new[]
            {
                
                new BsonDocument("$match", new BsonDocument
                {
                    { "month", new BsonDocument("$in", new BsonArray { 1, 2 }) },
                    { "day", new BsonDocument("$mod", new BsonArray { 2, 1 }) }
                }),
                
                
                new BsonDocument("$group", new BsonDocument
                {
                    { "_id", BsonNull.Value },
                    { "originalAvg", new BsonDocument("$avg", "$temperature") },
                    { "count", new BsonDocument("$sum", 1) }
                }),
                
                
                new BsonDocument("$project", new BsonDocument
                {
                    { "originalAvg", 1 },
                    { "newAvg", new BsonDocument("$add", new BsonArray { "$originalAvg", 1 }) },
                    { "difference", 1 },
                    { "_id", 0 }
                })
            };

            
            var result = weatherCollection.Aggregate<BsonDocument>(pipeline).ToList();

            
            foreach (var doc in result)
            {
                Console.WriteLine(doc.ToJson());
            }*/


            var pipeline = new[]
        {
            
            new BsonDocument("$match", new BsonDocument("$or", new BsonArray
            {
                new BsonDocument("month", new BsonDocument("$in", new BsonArray { 12, 1, 2 })),
                new BsonDocument("month", new BsonDocument("$in", new BsonArray { 6, 7, 8 }))
            })),

            
            new BsonDocument("$group", new BsonDocument
            {
                { "_id", new BsonDocument
                    {
                        { "year", "$year" },
                        { "month", "$month" },
                        { "day", "$day" }
                    }
                },
                { "avgTemp", new BsonDocument("$avg", "$temperature") }
            }),

            
            new BsonDocument("$facet", new BsonDocument
            {
                { "winter", new BsonArray
                    {
                        new BsonDocument("$match", new BsonDocument("_id.month", new BsonDocument("$in", new BsonArray { 12, 1, 2 }))),
                        new BsonDocument("$sort", new BsonDocument("avgTemp", -1)),
                        new BsonDocument("$limit", 1)
                    }
                },
                { "summer", new BsonArray
                    {
                        new BsonDocument("$match", new BsonDocument("_id.month", new BsonDocument("$in", new BsonArray { 6, 7, 8 }))),
                        new BsonDocument("$sort", new BsonDocument("avgTemp", 1)),
                        new BsonDocument("$limit", 1)
                    }
                }
            })
            };

            var result = weatherCollection.Aggregate<BsonDocument>(pipeline).ToList();

            foreach(var item in result)
            {
                Console.WriteLine(item.ToJson());
            }


            Console.ReadKey();
        }
    }
}
