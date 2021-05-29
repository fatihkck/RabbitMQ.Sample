using StackExchange.Redis;
using System;
using System.Text;

namespace RabbitMQ.Sample.Library.Cache.Redis
{
    public class RedisCacheManager : ICacheManager
    {
        private static IDatabase _db;
        private static readonly string host = "localhost";
        private static readonly int port = 6379;

        public RedisCacheManager()
        {
            CreateRedisDB();
        }

        private static IDatabase CreateRedisDB()
        {
            if (null == _db)
            {
                ConfigurationOptions option = new ConfigurationOptions();
                option.Ssl = false;
                option.EndPoints.Add(host, port);
                var connect = ConnectionMultiplexer.Connect(option);
                _db = connect.GetDatabase();
            }

            return _db;
        }

        public void Clear()
        {
            var server = _db.Multiplexer.GetServer(host, port);
            foreach (var item in server.Keys())
                _db.KeyDelete(item);
        }

        public string Get(string key)
        {
            var rValue = _db.SetMembers(key);
            if (rValue.Length == 0)
                return string.Empty;

            var result = rValue.ToStringArray();
            if (result.Length > 0)
                return result[0];

            return string.Empty;
        }


        public bool IsSet(string key)
        {
            return _db.KeyExists(key);
        }

        public bool Remove(string key)
        {
            return _db.KeyDelete(key);
        }

        public void RemoveByPattern(string pattern)
        {
            var server = _db.Multiplexer.GetServer(host, port);
            foreach (var item in server.Keys(pattern: "*" + pattern + "*"))
                _db.KeyDelete(item);
        }

        public void Set(string key, string data, int cacheTime = 0)
        {
            if (data == null)
                return;

            var entryBytes = Encoding.UTF8.GetBytes(data);
            _db.SetAdd(key, entryBytes);

            var expiresIn = TimeSpan.FromMinutes(cacheTime);

            if (cacheTime > 0)
                _db.KeyExpire(key, expiresIn);
        }
    }
}
