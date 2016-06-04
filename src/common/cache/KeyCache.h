//
// Created by pva701 on 03.06.16.
//

#ifndef ARDB_RELEASE_KEYCACHE_H
#define ARDB_RELEASE_KEYCACHE_H


#include <cstring>
#include <numeric>
#include <limits>
#include "sparsehash/dense_hash_map"

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

    struct KeyEntry {
        KeyType key;
        TtlType ttl;
        KeyEntry(const KeyType& key, TtlType ttl = INF): key(key), ttl(ttl) {}

        bool hasTTL() {
            return ttl != INF;
        }

        bool operator < (const KeyEntry& other) const {
            return ttl < other.ttl;
        }
    };

    KeyCache();
    void Put(const KeyType& kt);
    virtual void Put(const KeyEntry& keyEntry);
    virtual std::vector<KeyType> Get(const KeyType& pattern);
    virtual void Delete(const KeyType& key);
    virtual void Expire(const KeyType &key, TtlType ttl);
    size_t size();

    virtual ~KeyCache() {}

private:
    typedef google::dense_hash_map<KeyType, TtlType> HashMap;
    typedef std::set<KeyEntry> Set;
    Set sortedKeys;
    HashMap ttlByKey;
    void ensureTTL();

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

    /*struct SubstringMatcher : public Matcher {
        const KeyType& s;
        std::vector<int> prefix;
        SubstringMatcher(const KeyType& p): s(p), prefix(s.size()) {
            int n = (int) s.size();
            for (int i = 1; i < n; ++i) {
                int j = prefix[i - 1];
                while (j > 0 && s[i] != s[j]) j = prefix[j - 1];
                prefix[i] = j + (s[i] == s[j]);
            }
        }

        bool operator()(const KeyType& t) {
            if (t.size() < s.size())
                return false;
            int prev = prefix[s.size() - 1];
            int slen = s.size();
            for (int i = 0; i < t.size(); ++i) {
                int j = prev;
                while (j > 0 && t[i] != s[j]) j = prefix[j - 1];
                prev = j + (t[i] == s[j]);
                if (prev == slen)
                    return true;
            }
            return false;
        }
    };*/
};

#endif //ARDB_RELEASE_KEYCACHE_H
