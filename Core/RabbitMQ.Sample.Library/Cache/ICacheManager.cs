using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitMQ.Sample.Library.Cache
{
    public interface ICacheManager
    {
        string Get(string key);
        void Set(string key, string data, int cacheTime=0);
        bool IsSet(string key);
        bool Remove(string key);
        void RemoveByPattern(string pattern);
        void Clear();
    }
}
