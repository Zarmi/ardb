//
// Created by pva701 on 04.06.16.
//
#include "KeyCache.h"
#include "db/codec.hpp"
#include "db/engine.hpp"
#include <common/util/time_helper.hpp>

//Matchers
KeyCache::Matcher::~Matcher() {}
KeyCache::PrefixMatcher::PrefixMatcher(const KeyType& prefix): prefix(prefix) {}
KeyCache::SuffixMatcher::SuffixMatcher(const KeyType& suffix): suffix(suffix) {}
KeyCache::SubstringMatcher::SubstringMatcher(const KeyType& substring): substring(substring) {}
KeyCache::EqualsMatcher::EqualsMatcher(const KeyType& str): str(str) {}

bool KeyCache::PrefixMatcher::operator() (const KeyType& t) {
    return prefix.size() <= t.size() && t.substr(0, prefix.size()) == prefix;
}

bool KeyCache::SuffixMatcher::operator()(const KeyType& t)  {
    return suffix.size() <= t.size() && t.substr(t.size() - suffix.size(), suffix.size()) == suffix;
}

bool KeyCache::SubstringMatcher::operator()(const KeyType& t) {
    return t.find(substring) != -1;
}

bool KeyCache::EqualsMatcher::operator()(const KeyType& t) {
    return str == t;
}


//KeyCache implementation
KeyCache::KeyCache() {
    ttlByKey.set_empty_key("");
    ttlByKey.set_deleted_key("\n");
}

void KeyCache::LoadFromDisk() {
    /*int64_t match_count = 0;
    ardb::KeyObject startkey(ctx.ns, KEY_META, "");
    ctx.flags.iterate_multi_keys = 1;
    ctx.flags.iterate_no_upperbound = 1;
    ctx.flags.iterate_total_order = 1;
    ardb::Iterator* iter = m_engine->Find(ctx, startkey);
    while (iter->Valid())
    {
        KeyObject& k = iter->Key();
        if (k.GetType() == KEY_META)
        {
            std::string keystr;
            k.GetKey().AsString()
        }
        if (iter->Value().GetType() != KEY_STRING)
        {
            std::string keystr(k.GetKey().AsString());
            keystr.append(1, 0);
            KeyObject next(ctx.ns, KEY_META, keystr);
            iter->Jump(next);
            continue;
        }
        iter->Next();
    }
    DELETE(iter);*/
}

void KeyCache::Put(const KeyType& kt) {
    Put(CacheEntry(kt, INF));
}


void KeyCache::Put(const CacheEntry& keyEntry) {
    ensureTTL();
    if (sortedKeys.find(keyEntry.key) == sortedKeys.end()) {//!sortedKeys.contains(keyEntry.key)
        sortedKeys.insert(keyEntry);
        ttlByKey[keyEntry.key] = keyEntry.ttl;
    }
}

void KeyCache::Delete(const KeyType& key) {
    ensureTTL();
    typename HashMap::iterator it = ttlByKey.find(key);
    if (it != ttlByKey.end()) {//ttlByKey.contains(key)
        TtlType ttl = it->second;
        ttlByKey.erase(it);//erase ttl from ttl map

        sortedKeys.erase(CacheEntry(key, ttl));//erase KeyEntry from set
    }
}

std::vector<KeyCache::KeyType> KeyCache::Get(const KeyType& pattern) {
    ensureTTL();
    Matcher* matcher;
    if (pattern.size() != 1 && pattern[0] == '*' && pattern.back() == '*')
        matcher = new SubstringMatcher(pattern.substr(1, pattern.size() - 2));
    else if (pattern[0] == '*')
        matcher = new SuffixMatcher(pattern.substr(1, pattern.size() - 1));
    else if (pattern.back() == '*')
        matcher = new PrefixMatcher(pattern.substr(0, pattern.size() - 1));
    else
        matcher = new EqualsMatcher(pattern);

    std::vector<KeyType> ret;
    for (HashMap::const_iterator it = ttlByKey.begin(); it != ttlByKey.end(); it++)
        if ((*matcher)(it->first))
            ret.push_back(it->first);
    delete matcher;
    return ret;
}

void KeyCache::Expire(const KeyType &key, TtlType ttl) {
    ensureTTL();
    typename HashMap::iterator it = ttlByKey.find(key);
    if (it != ttlByKey.end()) {//ttlByKey.contains(key)
        TtlType prevTtl = it->second;
        sortedKeys.erase(CacheEntry(key, prevTtl));

        ttlByKey[key] = ttl;
        sortedKeys.insert(CacheEntry(key, ttl));
    }
}

size_t KeyCache::size() {
    ensureTTL();
    return sortedKeys.size();
}


void KeyCache::ensureTTL() {
    TtlType currentTime = ardb::get_current_epoch_millis();
    while (!sortedKeys.empty()) {
        CacheEntry entry = *sortedKeys.begin();
        if (entry.ttl > currentTime)
            return;
        sortedKeys.erase(sortedKeys.begin());
        ttlByKey.erase(entry.key);
    }
}

bool KeyCache::IsSupportedPattern(const KeyType& pattern) {
    for (int i = 1; i + 1 < pattern.size(); ++i)
        if (pattern[i] == '*' || pattern[i] == '?' || pattern[i] == '[' || pattern[i] == '\\')
            return false;
    if (pattern[0] == '?' || pattern[0] == '[' || pattern[0] == '\\')
        return false;
    if (pattern.back() == '?' || pattern.back() == '[' || pattern.back() == '\\')
        return false;
    return true;
}