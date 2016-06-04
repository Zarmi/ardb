//
// Created by pva701 on 04.06.16.
//
#include "KeyCache.h"
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

void KeyCache::Put(const KeyType& kt) {
    Put(KeyEntry(kt, INF));
}


void KeyCache::Put(const KeyEntry& keyEntry) {
    ensureTTL();
    sortedKeys.insert(keyEntry);
    ttlByKey[keyEntry.key] = keyEntry.ttl;
}

void KeyCache::Delete(const KeyType& key) {
    ensureTTL();
    typename HashMap::iterator it = ttlByKey.find(key);
    if (it != ttlByKey.end()) {
        TtlType ttl = it->second;
        ttlByKey.erase(it);//erase ttl from ttl map

        sortedKeys.erase({key, ttl});//erase KeyEntry from set
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
    if (it != ttlByKey.end()) {
        TtlType prevTtl = it->second;
        sortedKeys.erase({key, prevTtl});

        ttlByKey[key] = ttl;
        sortedKeys.insert({key, ttl});
    }
}

size_t KeyCache::size() {
    ensureTTL();
    return sortedKeys.size();
}


void KeyCache::ensureTTL() {
    TtlType currentTime = ardb::get_current_epoch_millis();
    while (!sortedKeys.empty()) {
        KeyEntry entry = *sortedKeys.begin();
        if (entry.ttl > currentTime)
            return;
        sortedKeys.erase(sortedKeys.begin());
        ttlByKey.erase(entry.key);
    }
}