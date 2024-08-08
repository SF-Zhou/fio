/*
 * mem engine
 *
 * IO engine that doesn't do any real IO transfers, it just pretends to.
 * The main purpose is to test fio itself.
 *
 * It also can act as external C++ engine - compiled with:
 *
 * g++ -O2 -g -shared -rdynamic -fPIC -o mem mem.cc \
 *	-include ../config-host.h -DFIO_EXTERNAL_ENGINE
 *
 * to test it execute:
 *
 * LD_LIBRARY_PATH=./engines ./fio examples/mem.fio
 *
 */
#include <assert.h>
#include <memory>
#include <stdlib.h>

#include "../config-host.h"
#include "../fio.h"

struct NullData {
  NullData(struct thread_data *td) {
    if (td->o.iodepth != 1) {
      io_us_ = std::make_unique<struct io_u *[]>(td->o.iodepth);
      td->io_ops->flags |= FIO_ASYNCIO_SETS_ISSUE_TIME;
    } else {
      td->io_ops->flags |= FIO_SYNCIO;
    }

    td_set_ioengine_flags(td);
  }

  static NullData *get(struct thread_data *td) {
    return reinterpret_cast<NullData *>(td->io_ops_data);
  }

  io_u *fio_mem_event(struct thread_data *, int event) { return io_us_[event]; }

  int fio_mem_getevents(struct thread_data *, unsigned int min_events,
                        unsigned int max, const struct timespec *t) {
    int ret = 0;

    if (min_events) {
      ret = events_;
      events_ = 0;
    }

    return ret;
  }

  int fio_mem_commit(struct thread_data *td) {
    if (!events_) {
      if (fio_fill_issue_time(td)) {
        struct timespec now;
        fio_gettime(&now, NULL);

        for (int i = 0; i < queued_; i++) {
          struct io_u *io_u = io_us_[i];

          memcpy(&io_u->issue_time, &now, sizeof(now));
          io_u_queued(td, io_u);
        }
      }

      events_ = queued_;
      queued_ = 0;
    }
    return 0;
  }

  fio_q_status fio_mem_queue(struct thread_data *td, struct io_u *io_u) {
    fio_ro_check(td, io_u);

    if (td->io_ops->flags & FIO_SYNCIO) {
      return FIO_Q_COMPLETED;
    }
    if (events_) {
      return FIO_Q_BUSY;
    }

    io_us_[queued_++] = io_u;
    return FIO_Q_QUEUED;
  }

  int fio_mem_open(struct thread_data *, struct fio_file *f) { return 0; }

private:
  std::unique_ptr<struct io_u *[]> io_us_;
  int queued_;
  int events_;
};

extern "C" {

static struct io_u *fio_mem_event(struct thread_data *td, int event) {
  return NullData::get(td)->fio_mem_event(td, event);
}

static int fio_mem_getevents(struct thread_data *td, unsigned int min_events,
                             unsigned int max, const struct timespec *t) {
  return NullData::get(td)->fio_mem_getevents(td, min_events, max, t);
}

static int fio_mem_commit(struct thread_data *td) {
  return NullData::get(td)->fio_mem_commit(td);
}

static fio_q_status fio_mem_queue(struct thread_data *td, struct io_u *io_u) {
  return NullData::get(td)->fio_mem_queue(td, io_u);
}

static int fio_mem_open(struct thread_data *td, struct fio_file *f) {
  return NullData::get(td)->fio_mem_open(td, f);
}

static int fio_mem_init(struct thread_data *td) {
  td->io_ops_data = new NullData(td);
  return 0;
}

static void fio_mem_cleanup(struct thread_data *td) {
  delete NullData::get(td);
}

static struct ioengine_ops ioengine;
void get_ioengine(struct ioengine_ops **ioengine_ptr) {
  *ioengine_ptr = &ioengine;

  ioengine.name = "mem";
  ioengine.version = FIO_IOOPS_VERSION;
  ioengine.queue = fio_mem_queue;
  ioengine.commit = fio_mem_commit;
  ioengine.getevents = fio_mem_getevents;
  ioengine.event = fio_mem_event;
  ioengine.init = fio_mem_init;
  ioengine.cleanup = fio_mem_cleanup;
  ioengine.open_file = fio_mem_open;
  ioengine.flags = FIO_DISKLESSIO | FIO_FAKEIO;
}
}
