// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Logger implementation that can be shared by all environments
// where enough posix functionality is available.

#ifndef STORAGE_LEVELDB_UTIL_POSIX_LOGGER_H_
#define STORAGE_LEVELDB_UTIL_POSIX_LOGGER_H_

#include <algorithm>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include "pebblesdb/env.h"

#include "libpmem.h"
#include <atomic>
#include "port/port.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-format-attribute"

namespace leveldb {

class PmdkLogger : public Logger {
 private:
  PmdkLogger(const PmdkLogger&);
  PmdkLogger& operator = (const PmdkLogger&);
  //FILE* file_;
  uint64_t (*gettid_)();  // Return the thread id for the current thread
  void *file_space_;
  size_t size_;
  size_t now_off_;
  std::atomic<bool> truncating_;
  std::string filename_;
  port::Mutex mtx_;
 public:
  PmdkLogger(const std::string& fname, void *file_space, size_t size, uint64_t (*gettid)()) : 
             filename_(fname), file_space_(file_space), size_(size), 
             now_off_(0), gettid_(gettid), truncating_(false), mtx_() { }
  virtual ~PmdkLogger() {
    pmem_unmap(file_space_, size_);
  }
  virtual void Logv(const char* format, va_list ap) {
    const uint64_t thread_id = (*gettid_)();
    
    // We try twice: the first time with a fixed-size stack allocated buffer,
    // and the second time with a much larger dynamically allocated buffer.
    char buffer[500];
    for (int iter = 0; iter < 2; iter++) {
      char* base;
      int bufsize;
      if (iter == 0) {
        bufsize = sizeof(buffer);
        base = buffer;
      } else {
        bufsize = 30000;
        base = new char[bufsize];
      }
      char* p = base;
      char* limit = base + bufsize;

      struct timeval now_tv;
      gettimeofday(&now_tv, NULL);
      const time_t seconds = now_tv.tv_sec;
      struct tm t;
      localtime_r(&seconds, &t);
      p += snprintf(p, limit - p,
                    "%04d/%02d/%02d-%02d:%02d:%02d.%06d %llx ",
                    t.tm_year + 1900,
                    t.tm_mon + 1,
                    t.tm_mday,
                    t.tm_hour,
                    t.tm_min,
                    t.tm_sec,
                    static_cast<int>(now_tv.tv_usec),
                    static_cast<long long unsigned int>(thread_id));

      // Print the message
      if (p < limit) {
        va_list backup_ap;
        va_copy(backup_ap, ap);
        p += vsnprintf(p, limit - p, format, backup_ap);
        va_end(backup_ap);
      }

      // Truncate to available space if necessary
      if (p >= limit) {
        if (iter == 0) {
          continue;       // Try again with larger buffer
        } else {
          p = limit - 1;
        }
      }

      // Add newline if necessary
      if (p == base || p[-1] != '\n') {
        *p++ = '\n';
      }

      assert(p <= limit);
      //fwrite(base, 1, p - base, file_);
      //fflush(file_);
      mtx_.Lock();
      while (now_off_ + (p - base) >= size_) {
        while(truncating_.load(std::memory_order_acquire)) {}
        truncating_.store(true, std::memory_order_release);
        pmem_unmap(file_space_, size_);
        size_t new_size;
        pmem_map_file(filename_.c_str(), size_+4*1024, 
                      PMEM_FILE_CREATE | PMEM_FILE_SPARSE,
                      0666, &new_size, NULL);
        size_ = new_size;
        truncating_.store(false, std::memory_order_release);
      }
      pmem_memcpy(reinterpret_cast<char *>(file_space_)+now_off_,
                  base, p - base, PMEM_F_MEM_NONTEMPORAL);
      mtx_.Unlock();
      if (base != buffer) {
        delete[] base;
      }
      break;
    }
  }
};

}  // namespace leveldb

#pragma GCC diagnostic pop

#endif  // STORAGE_LEVELDB_UTIL_POSIX_LOGGER_H_
