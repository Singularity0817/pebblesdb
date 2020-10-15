#include <iostream>

#include "pebblesdb/env.h"
#include "pebblesdb/slice.h"

using namespace leveldb;

int main()
{
    leveldb::Env *env_ = leveldb::Env::Default();
    leveldb::WritableFile *wf;
    env_->NewWritableFile("/mnt/pmem0/zwh_test/pebblesdb/1.txt", &wf);
    Slice s("li\0xi\0", 6);
    for (int i = 0; i < 5000; i++) {
        wf->Append(s);
    }
    wf->Close();
    delete wf;
    leveldb::RandomAccessFile *rf;
    env_->NewRandomAccessFile("/mnt/pmem0/zwh_test/pebblesdb/1.txt", &rf);
    char *scratch = (char *)malloc(3*sizeof(char));
    Slice n_s;
    for (int i = 0; i < 3; i++) {
        rf->Read(3*i, 3, &n_s, scratch);
        printf("stored %s\n", n_s.data());
    }
    delete rf;
    return 0;
}