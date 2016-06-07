//
// Created by pva701 on 04.06.16.
//

#ifndef ARDB_RELEASE_CONCURRENTKEYCACHE_H
#define ARDB_RELEASE_CONCURRENTKEYCACHE_H

#include "KeyCache.h"
#include "thread/spin_rwlock.hpp"
#include "thread/spin_mutex_lock.hpp"

class ConcurrentKeyCache: public KeyCache {
    void Put(const CacheEntry& keyEntry);
    std::vector<KeyType> Get(const KeyType& pattern);
    void Delete(const KeyType& key);
    void Expire(const KeyType &key, TtlType ttl);
    size_t size();
    void DropAll();
protected:
    virtual void ensureTTL();

    ardb::SpinRWLock lock;
    ardb::SpinMutexLock ensureTtlLock;
};


#endif //ARDB_RELEASE_CONCURRENTKEYCACHE_H
