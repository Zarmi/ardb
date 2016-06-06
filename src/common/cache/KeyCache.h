//
// Created by pva701 on 03.06.16.
//

#ifndef ARDB_RELEASE_KEYCACHE_H
#define ARDB_RELEASE_KEYCACHE_H


#include <cstring>
#include <numeric>
#include <limits>
#include "sparsehash/dense_hash_map"
#include "db/engine.hpp"

#include <cstring>
#include <numeric>
#include <limits>
#include "common.hpp"
#include <vector>
#include <set>


class KeyCache {
public:
    typedef std::string KeyType;
    typedef int64_t TtlType;
    static const TtlType INF = std::numeric_limits<TtlType>::max();

    struct CacheEntry {
        KeyType key;
        TtlType ttl;
        CacheEntry(const KeyType& key, TtlType ttl = INF): key(key), ttl(ttl < 0 ? INF: ttl) {}

        bool hasTTL() {
            return ttl != INF;
        }

        bool operator < (const CacheEntry& other) const {
            return ttl < other.ttl || ttl == other.ttl && key < other.key;
        }
    };

    KeyCache();
    void LoadFromDisk(ardb::Engine* engine);
    virtual std::vector<KeyType> Get(const KeyType& pattern);
    void Put(const KeyType& kt);
    virtual void Put(const CacheEntry& keyEntry);
    virtual void Delete(const KeyType& key);
    virtual void Expire(const KeyType &key, TtlType ttl);
    virtual bool IsSupportedPattern(const KeyType& pattern);
    virtual size_t size();

    virtual ~KeyCache() {}

protected:
    typedef google::dense_hash_map<KeyType, TtlType> HashMap;
    typedef std::set<CacheEntry> Set;
    Set sortedKeys;
    HashMap ttlByKey;
    virtual void ensureTTL();

    struct Matcher {
        virtual bool operator() (const KeyType& s) = 0;
        virtual ~Matcher();
    };

    struct PrefixMatcher : public Matcher {
        KeyType prefix;
        PrefixMatcher(const KeyType& prefix);
        bool operator() (const KeyType& t);
    };

    struct SuffixMatcher : public Matcher {
        KeyType suffix;
        SuffixMatcher(const KeyType& suffix);
        bool operator()(const KeyType& t);
    };

    struct SubstringMatcher : public Matcher {
        KeyType substring;
        SubstringMatcher(const KeyType& substring);
        bool operator()(const KeyType& t);
    };

    struct EqualsMatcher : public Matcher {
        KeyType str;
        EqualsMatcher(const KeyType& str);
        bool operator()(const KeyType& t);
    };
};

#endif //ARDB_RELEASE_KEYCACHE_H
