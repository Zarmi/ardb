//
// Created by pva701 on 04.06.16.
//

#include <common/thread/lock_guard.hpp>
#include "ConcurrentKeyCache.h"
//LockGuard<SpinMutexLock> guard(m_expires_lock);

void ConcurrentKeyCache::Put(const CacheEntry& keyEntry) {
    ardb::WriteLockGuard<ardb::SpinRWLock> guard(lock);
    KeyCache::Put(keyEntry);
}

std::vector<KeyCache::KeyType> ConcurrentKeyCache::Get(const KeyType& pattern) {
    ardb::ReadLockGuard<ardb::SpinRWLock> guard(lock);
    return KeyCache::Get(pattern);
}

void ConcurrentKeyCache::Delete(const KeyType& key) {
    ardb::WriteLockGuard<ardb::SpinRWLock> guard(lock);
    KeyCache::Delete(key);
}

void ConcurrentKeyCache::Expire(const KeyType &key, TtlType ttl) {
    ardb::WriteLockGuard<ardb::SpinRWLock> guard(lock);
    KeyCache::Expire(key, ttl);
}

size_t ConcurrentKeyCache::size() {
    ardb::ReadLockGuard<ardb::SpinRWLock> guard(lock);
    return KeyCache::size();
}

void ConcurrentKeyCache::ensureTTL() {
    ardb::LockGuard<ardb::SpinMutexLock> guard(ensureTtlLock);
    KeyCache::ensureTTL();
}

void ConcurrentKeyCache::DropAll() {
    ardb::WriteLockGuard<ardb::SpinRWLock> guard(lock);
    KeyCache::DropAll();
}