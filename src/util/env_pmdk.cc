// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <deque>
#include <set>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#if defined(LEVELDB_PLATFORM_ANDROID)
#include <sys/stat.h>
#endif
#include "pebblesdb/env.h"
#include "pebblesdb/slice.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/mutexlock.h"

#include "util/pmdk_logger.h"
#include "libpmem.h"

#define FILE_INCREMENT_SIZE ((1024*1024))

namespace leveldb {

namespace {

static Status IOError(const std::string& context, int err_number) {
  return Status::IOError(context, strerror(err_number));
}

class PmdkSequentialFile: public SequentialFile {
 private:
  PmdkSequentialFile(const PmdkSequentialFile&);
  PmdkSequentialFile& operator = (const PmdkSequentialFile&);
  std::string filename_;
  void *base_;
  size_t size_;
  size_t now_off_;

 public:
  PmdkSequentialFile(const std::string& fname, void *base, size_t size)
      : filename_(fname), base_(base), size_(size), now_off_(0) { }
  virtual ~PmdkSequentialFile() { pmem_unmap(base_, size_); }

  virtual Status Read(size_t n, Slice* result, char* scratch) {
    if (now_off_ + n > size_) {
      return IOError(filename_, errno);
    }
    memcpy(scratch, reinterpret_cast<char *>(base_)+now_off_, n);
    now_off_ += n;
    *result = Slice(scratch, n);
    return Status::OK();
  }

  virtual Status Skip(uint64_t n) {
    if (now_off_ + n > size_) {
      return IOError(filename_, errno);
    }
    now_off_ += n;
    return Status::OK();
  }
};

// pread() based random-access
class PmdkRandomAccessFile: public RandomAccessFile {
 private:
  std::string filename_;
  int fd_;
  void *base_;
  size_t size_;

 public:
  PmdkRandomAccessFile(const std::string& fname, void *base, size_t size)
      : filename_(fname), base_(base), size_(size) { }
  virtual ~PmdkRandomAccessFile() { pmem_unmap(base_, size_); }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    if (offset + n > size_) {
      return IOError(filename_, errno);
    }
    memcpy(scratch, reinterpret_cast<char*>(base_)+offset, n);
    *result = Slice(scratch, n);
    return Status::OK();
  }
};

// mmap() based random-access
class PmdkMmapReadableFile: public RandomAccessFile {
 private:
  PmdkMmapReadableFile(const PmdkMmapReadableFile&);
  PmdkMmapReadableFile& operator = (const PmdkMmapReadableFile&);
  std::string filename_;
  void* mmapped_region_;
  size_t length_;

 public:
  // base[0,length-1] contains the mmapped contents of the file.
  PmdkMmapReadableFile(const std::string& fname, void* base, size_t length)
      : filename_(fname), mmapped_region_(base), length_(length) { }

  virtual ~PmdkMmapReadableFile() {
    pmem_unmap(mmapped_region_, length_);
  }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    Status s;
    if (offset + n > length_) {
      *result = Slice();
      s = IOError(filename_, EINVAL);
    } else {
      memcpy(scratch, reinterpret_cast<char*>(mmapped_region_) + offset, n);
      //*result = Slice(reinterpret_cast<char*>(mmapped_region_) + offset, n);
      *result = Slice(scratch, n);
    }
    return s;
  }
};

class PmdkWritableFile : public WritableFile {
 private:
  PmdkWritableFile(const PmdkWritableFile&);
  PmdkWritableFile& operator = (const PmdkWritableFile&);
  std::string filename_;
  void* base_;
  size_t size_;
  size_t now_off_;

 public:
  PmdkWritableFile(const std::string& fname, void *base, size_t size)
      : filename_(fname), base_(base), size_(size), now_off_(0) { }

  ~PmdkWritableFile() {
    if (base_ != NULL) {
      this->Close();
    }
  }

  virtual Status Append(const Slice& data) {
    while (now_off_ + data.size() >= size_) {
      size_t new_size;
      pmem_unmap(base_, size_);
      base_ = pmem_map_file(filename_.c_str(), 
                            size_+FILE_INCREMENT_SIZE, 
                            PMEM_FILE_CREATE | PMEM_FILE_SPARSE,
                            0666, &new_size, NULL);
      size_ = new_size;
    }
    pmem_memcpy(reinterpret_cast<char*>(base_)+now_off_, data.data(), 
                data.size(), PMEM_F_MEM_NONTEMPORAL);
    now_off_ += data.size();
    return Status::OK();
  }

  virtual Status Close() {
    if (base_ != NULL) {
      pmem_unmap(base_, size_);
      base_ = pmem_map_file(filename_.c_str(), 
                            now_off_, 
                            PMEM_FILE_CREATE | PMEM_FILE_SPARSE,
                            0666, NULL, NULL);
      size_ = now_off_;
      pmem_unmap(base_, size_);
      base_ = NULL;
      size_ = 0;
    }
    return Status::OK();
  }

  virtual Status Flush() {
    pmem_persist(base_, size_);
    return Status::OK();
  }

  Status SyncDirIfManifest() {
    const char* f = filename_.c_str();
    const char* sep = strrchr(f, '/');
    Slice basename;
    std::string dir;
    if (sep == NULL) {
      dir = ".";
      basename = f;
    } else {
      dir = std::string(f, sep - f);
      basename = sep + 1;
    }
    Status s;
    if (basename.starts_with("MANIFEST")) {
      int fd = open(dir.c_str(), O_RDONLY);
      if (fd < 0) {
        s = IOError(dir, errno);
      } else {
        if (fsync(fd) < 0) {
          s = IOError(dir, errno);
        }
        close(fd);
      }
    }
    return s;
  }

  virtual Status Sync() {
    // Ensure new files referred to by the manifest are in the filesystem.
    Status s = SyncDirIfManifest();
    if (!s.ok()) {
      return s;
    }
    pmem_persist(base_, size_);
    return s;
  }
};

// We preallocate up to an extra megabyte and use memcpy to append new
// data to the file.  This is safe since we either properly close the
// file before reading from it, or for log files, the reading code
// knows enough to skip zero suffixes.
class PmdkMmapFile : public ConcurrentWritableFile {
 private:
  PmdkMmapFile(const PmdkMmapFile&);
  PmdkMmapFile& operator = (const PmdkMmapFile&);

  std::string filename_;    // Path to the file
  uint64_t end_offset_;     // Where does the file end?
  bool trunc_in_progress_;  // is there an ongoing truncate operation?
  port::Mutex mtx_;         // Protection for state
  port::CondVar cnd_;       // Wait for truncate
  size_t file_size_;        // Size of the file
  void *base_;              // The mapped file

  // Roundup x to a multiple of y
  static size_t Roundup(size_t x, size_t y) {
    return ((x + y - 1) / y) * y;
  }

 public:
  PmdkMmapFile(const std::string& fname, void* base, size_t file_size)
      : filename_(fname),
        base_(base),
        file_size_(file_size),
        end_offset_(0),
        trunc_in_progress_(false),
        mtx_(),
        cnd_(&mtx_) {}

  ~PmdkMmapFile() {
    PmdkMmapFile::Close();
  }

  virtual Status WriteAt(uint64_t offset, const Slice& data) {
    const uint64_t end = offset + data.size();
    const char* src = data.data();
    uint64_t rem = data.size();
    mtx_.Lock();
    end_offset_ = end_offset_ < end ? end : end_offset_;
    mtx_.Unlock();
    
    while (file_size_ < end_offset_) {
      mtx_.Lock();
      trunc_in_progress_ = file_size_ < end_offset_;
      if (trunc_in_progress_) {
        size_t new_file_size;
        pmem_unmap(base_, file_size_);
        base_ = pmem_map_file(filename_.c_str(), 
                              file_size_ + FILE_INCREMENT_SIZE, 
                              PMEM_FILE_CREATE | PMEM_FILE_SPARSE, 
                              0666, &new_file_size, NULL);
        file_size_ = new_file_size;
        trunc_in_progress_ = false;
      }
      mtx_.Unlock();
    }
    pmem_memcpy(reinterpret_cast<char*>(base_) + offset, src, rem, PMEM_F_MEM_NONTEMPORAL);
    return Status::OK();
  }

  virtual Status Append(const Slice& data) {
    mtx_.Lock();
    uint64_t offset = end_offset_;
    mtx_.Unlock();
    return WriteAt(offset, data);
  }

  virtual Status Close() {
    mtx_.Lock();
    pmem_unmap(base_, file_size_);
    base_ = pmem_map_file(filename_.c_str(),
                          end_offset_,
                          PMEM_FILE_CREATE,
                          0666, NULL, NULL);
    file_size_ = end_offset_;
    pmem_unmap(base_, file_size_);
    base_ = NULL;
    mtx_.Unlock();
    return Status::OK();
  }

  Status Flush() {
    pmem_persist(base_, end_offset_);
    return Status::OK();
  }

  Status SyncDirIfManifest() {
    const char* f = filename_.c_str();
    const char* sep = strrchr(f, '/');
    Slice basename;
    std::string dir;
    if (sep == NULL) {
      dir = ".";
      basename = f;
    } else {
      dir = std::string(f, sep - f);
      basename = sep + 1;
    }
    Status s;
    if (basename.starts_with("MANIFEST")) {
      int fd = open(dir.c_str(), O_RDONLY);
      if (fd < 0) {
        s = IOError(dir, errno);
      } else {
        if (fsync(fd) < 0) {
          s = IOError(dir, errno);
        }
        close(fd);
      }
    }
    return s;
  }

  virtual Status Sync() {
    // Ensure new files referred to by the manifest are in the filesystem.
    Status s = SyncDirIfManifest();
    if (!s.ok()) {
      return s;
    }
    pmem_persist(base_, end_offset_);
  }
};

static int LockOrUnlock(int fd, bool lock) {
  errno = 0;
  struct flock f;
  memset(&f, 0, sizeof(f));
  f.l_type = (lock ? F_WRLCK : F_UNLCK);
  f.l_whence = SEEK_SET;
  f.l_start = 0;
  f.l_len = 0;        // Lock/unlock entire file
  return fcntl(fd, F_SETLK, &f);
}

class PosixFileLock : public FileLock {
 public:
  PosixFileLock() : fd_(-1), name_() { }
  int fd_;
  std::string name_;
};

// Set of locked files.  We keep a separate set instead of just
// relying on fcntrl(F_SETLK) since fcntl(F_SETLK) does not provide
// any protection against multiple uses from the same process.
class PosixLockTable {
 private:
  port::Mutex mu_;
  std::set<std::string> locked_files_;
 public:
  PosixLockTable() : mu_(), locked_files_() { }
  bool Insert(const std::string& fname) {
    MutexLock l(&mu_);
    return locked_files_.insert(fname).second;
  }
  void Remove(const std::string& fname) {
    MutexLock l(&mu_);
    locked_files_.erase(fname);
  }
};

class PmdkEnv : public Env {
 public:
  PmdkEnv();
  virtual ~PmdkEnv() {
    fprintf(stderr, "Destroying Env::Default()\n");
    abort();
  }

  virtual Status NewSequentialFile(const std::string& fname,
                                   SequentialFile** result) {
    size_t size;
    void *base = pmem_map_file(fname.c_str(), 0, 0, 0, &size, NULL);
    if (base == NULL) {
      *result = NULL;
      return IOError(fname, errno);
    } else {
      *result = new PmdkSequentialFile(fname, base, size);
      return Status::OK();
    }
  }

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     RandomAccessFile** result) {
    *result = NULL;
    size_t size;
    void* base = pmem_map_file(fname.c_str(), 0, 0, 0, &size, NULL);
    if (base == NULL) {
      return IOError(fname, errno);
    }
    *result = new PmdkMmapReadableFile(fname, base, size);
    return Status::OK();
  }

  virtual Status NewWritableFile(const std::string& fname,
                                 WritableFile** result) {
    size_t size;
    void *base = pmem_map_file(fname.c_str(), 
                               FILE_INCREMENT_SIZE/*size*/, 
                               PMEM_FILE_CREATE, 
                               0666, &size, NULL);
    if (base == NULL) {
      *result = NULL;
      return IOError(fname, errno);
    } else {
      *result = new PmdkWritableFile(fname, base, size);
    }
    return Status::OK();
  }

  virtual Status NewConcurrentWritableFile(const std::string& fname,
                                           ConcurrentWritableFile** result) {
    size_t size;
    void *base = pmem_map_file(fname.c_str(), 
                               FILE_INCREMENT_SIZE/*size*/, 
                               PMEM_FILE_CREATE, 
                               0666, &size, NULL);
    if (base == NULL) {
      *result = NULL;
      return IOError(fname, errno);
    } else {
      *result = new PmdkMmapFile(fname, base, size);
    }
    return Status::OK();
  }

  virtual bool FileExists(const std::string& fname) {
    return access(fname.c_str(), F_OK) == 0;
  }

  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) {
    result->clear();
    DIR* d = opendir(dir.c_str());
    if (d == NULL) {
      return IOError(dir, errno);
    }
    struct dirent* entry;
    while ((entry = readdir(d)) != NULL) {
      result->push_back(entry->d_name);
    }
    closedir(d);
    return Status::OK();
  }

  virtual Status DeleteFile(const std::string& fname) {
    Status result;
    if (unlink(fname.c_str()) != 0) {
      result = IOError(fname, errno);
    }
    return result;
  }

  virtual Status CreateDir(const std::string& name) {
    Status result;
    if (mkdir(name.c_str(), 0755) != 0) {
      result = IOError(name, errno);
    }
    return result;
  }

  virtual Status DeleteDir(const std::string& name) {
    Status result;
    if (rmdir(name.c_str()) != 0) {
      result = IOError(name, errno);
    }
    return result;
  }

  virtual Status GetFileSize(const std::string& fname, uint64_t* size) {
    Status s;
    struct stat sbuf;
    if (stat(fname.c_str(), &sbuf) != 0) {
      *size = 0;
      s = IOError(fname, errno);
    } else {
      *size = sbuf.st_size;
    }
    return s;
  }

  virtual Status RenameFile(const std::string& src, const std::string& target) {
    Status result;
    if (rename(src.c_str(), target.c_str()) != 0) {
      result = IOError(src, errno);
    }
    return result;
  }

  virtual Status CopyFile(const std::string& src, const std::string& target) {
    Status result;
    int fd1 = -1;
    int fd2 = -1;

    if (result.ok() && (fd1 = open(src.c_str(), O_RDONLY)) < 0) {
      result = IOError(src, errno);
    }
    if (result.ok() && (fd2 = open(target.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644)) < 0) {
      result = IOError(target, errno);
    }

    ssize_t amt = 0;
    char buf[512];

    while (result.ok() && (amt = read(fd1, buf, 512)) > 0) {
      if (write(fd2, buf, amt) != amt) {
        result = IOError(src, errno);
      }
    }

    if (result.ok() && amt < 0) {
      result = IOError(src, errno);
    }

    if (fd1 >= 0 && close(fd1) < 0) {
      if (result.ok()) {
        result = IOError(src, errno);
      }
    }

    if (fd2 >= 0 && close(fd2) < 0) {
      if (result.ok()) {
        result = IOError(target, errno);
      }
    }

    return result;
  }

  virtual Status LinkFile(const std::string& src, const std::string& target) {
    Status result;
    if (link(src.c_str(), target.c_str()) != 0) {
      result = IOError(src, errno);
    }
    return result;
  }

  virtual Status LockFile(const std::string& fname, FileLock** lock) {
    *lock = NULL;
    Status result;
    int fd = open(fname.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
      result = IOError(fname, errno);
    } else if (!locks_.Insert(fname)) {
      close(fd);
      result = Status::IOError("lock " + fname, "already held by process");
    } else if (LockOrUnlock(fd, true) == -1) {
      result = IOError("lock " + fname, errno);
      close(fd);
      locks_.Remove(fname);
    } else {
      PosixFileLock* my_lock = new PosixFileLock;
      my_lock->fd_ = fd;
      my_lock->name_ = fname;
      *lock = my_lock;
    }
    return result;
  }

  virtual Status UnlockFile(FileLock* lock) {
    PosixFileLock* my_lock = reinterpret_cast<PosixFileLock*>(lock);
    Status result;
    if (LockOrUnlock(my_lock->fd_, false) == -1) {
      result = IOError("unlock", errno);
    }
    locks_.Remove(my_lock->name_);
    close(my_lock->fd_);
    delete my_lock;
    return result;
  }

  virtual void Schedule(void (*function)(void*), void* arg);

  virtual void StartThread(void (*function)(void* arg), void* arg);

  virtual pthread_t StartThreadAndReturnThreadId(void (*function)(void* arg), void* arg);

  virtual void WaitForThread(unsigned long int th, void** return_status);

  virtual Status GetTestDirectory(std::string* result) {
    const char* env = getenv("TEST_TMPDIR");
    if (env && env[0] != '\0') {
      *result = env;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "/tmp/leveldbtest-%d", int(geteuid()));
      *result = buf;
    }
    // Directory may already exist
    CreateDir(*result);
    return Status::OK();
  }

  static uint64_t gettid() {
    pthread_t tid = pthread_self();
    uint64_t thread_id = 0;
    memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
    return thread_id;
  }

  virtual Status NewLogger(const std::string& fname, Logger** result) {
    //FILE* f = fopen(fname.c_str(), "w");
    size_t size;
    void *base = pmem_map_file(fname.c_str(), FILE_INCREMENT_SIZE, PMEM_FILE_CREATE, 0666, &size, NULL);
    if (base == NULL) {
      *result = NULL;
      return IOError(fname, errno);
    } else {
      *result = new PmdkLogger(fname, base, size, &PmdkEnv::gettid);
      return Status::OK();
    }
  }

  virtual uint64_t NowMicros() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
  }

  virtual void SleepForMicroseconds(int micros) {
    usleep(micros);
  }

  virtual pthread_t GetThreadId() {
	pthread_t tid = pthread_self();
	return tid;
  }

 private:
  void PthreadCall(const char* label, int result) {
    if (result != 0) {
      fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
      abort();
    }
  }

  // BGThread() is the body of the background thread
  void BGThread();
  static void* BGThreadWrapper(void* arg) {
    reinterpret_cast<PmdkEnv*>(arg)->BGThread();
    return NULL;
  }

  size_t page_size_;
  pthread_mutex_t mu_;
  pthread_cond_t bgsignal_;
  pthread_t bgthread_;
  bool started_bgthread_;

  // Entry per Schedule() call
  struct BGItem { void* arg; void (*function)(void*); };
  typedef std::deque<BGItem> BGQueue;
  BGQueue queue_;

  PosixLockTable locks_;
  //MmapLimiter mmap_limit_;
};

PmdkEnv::PmdkEnv() : page_size_(getpagesize()),
                       mu_(),
                       bgsignal_(),
                       bgthread_(),
                       started_bgthread_(false),
                       queue_(),
                       locks_() {
  PthreadCall("mutex_init", pthread_mutex_init(&mu_, NULL));
  PthreadCall("cvar_init", pthread_cond_init(&bgsignal_, NULL));
}

void PmdkEnv::Schedule(void (*function)(void*), void* arg) {
  PthreadCall("lock", pthread_mutex_lock(&mu_));

  // Start background thread if necessary
  if (!started_bgthread_) {
    started_bgthread_ = true;
    PthreadCall(
        "create thread",
        pthread_create(&bgthread_, NULL,  &PmdkEnv::BGThreadWrapper, this));
  }

  // If the queue is currently empty, the background thread may currently be
  // waiting.
  if (queue_.empty()) {
    PthreadCall("signal", pthread_cond_signal(&bgsignal_));
  }

  // Add to priority queue
  queue_.push_back(BGItem());
  queue_.back().function = function;
  queue_.back().arg = arg;

  PthreadCall("unlock", pthread_mutex_unlock(&mu_));
}

void PmdkEnv::BGThread() {
  while (true) {
    // Wait until there is an item that is ready to run
    PthreadCall("lock", pthread_mutex_lock(&mu_));
    while (queue_.empty()) {
      PthreadCall("wait", pthread_cond_wait(&bgsignal_, &mu_));
    }

    void (*function)(void*) = queue_.front().function;
    void* arg = queue_.front().arg;
    queue_.pop_front();

    PthreadCall("unlock", pthread_mutex_unlock(&mu_));
    (*function)(arg);
  }
}

namespace {
struct StartThreadState {
  void (*user_function)(void*);
  void* arg;
};
}
static void* StartThreadWrapper(void* arg) {
  StartThreadState* state = reinterpret_cast<StartThreadState*>(arg);
  state->user_function(state->arg);
  delete state;
  return NULL;
}

void PmdkEnv::StartThread(void (*function)(void* arg), void* arg) {
  pthread_t t;
  StartThreadState* state = new StartThreadState;
  state->user_function = function;
  state->arg = arg;
  PthreadCall("start thread",
              pthread_create(&t, NULL,  &StartThreadWrapper, state));
}

pthread_t PmdkEnv::StartThreadAndReturnThreadId(void (*function)(void* arg), void* arg) {
  pthread_t t;
  StartThreadState* state = new StartThreadState;
  state->user_function = function;
  state->arg = arg;
  PthreadCall("start thread",
              pthread_create(&t, NULL,  &StartThreadWrapper, state));
  return t;
}

void PmdkEnv::WaitForThread(unsigned long int th, void** return_status) {
  PthreadCall("wait for thread",
              pthread_join((pthread_t)th, return_status));
}
}  // namespace

static pthread_once_t once = PTHREAD_ONCE_INIT;
static Env* default_env;
static void InitDefaultEnv() { default_env = new PmdkEnv; }

Env* Env::Default() {
  pthread_once(&once, InitDefaultEnv);
  return default_env;
}

}  // namespace leveldb
